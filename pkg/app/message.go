package app

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vmihailenco/msgpack/v5"
	goproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/birdayz/kaf/pkg/proto"
)

// MessageHeader is a JSON-serializable record header.
type MessageHeader struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// JSONEachRowMessage is the wire format for --output json-each-row / --input json-each-row.
type JSONEachRowMessage struct {
	Topic     string          `json:"topic"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Timestamp time.Time       `json:"timestamp"`
	Headers   []MessageHeader `json:"headers"`
	Key       string          `json:"key"`
	Payload   string          `json:"payload"`
}

// HandleMessage decodes, formats, and prints a single consumed record.
// It is used by both the consume command and the group peek command.
func (a *App) HandleMessage(rec *kgo.Record, mu *sync.Mutex, outputFmt OutputFormat, headerFilter map[string]string) {
	if !CheckHeaders(rec.Headers, headerFilter) {
		return
	}

	var stderr bytes.Buffer

	var dataToDisplay []byte
	var keyToDisplay []byte
	var err error

	if a.ProtoType != "" {
		dataToDisplay, err = ProtoDecode(a.Reg, rec.Value, a.ProtoType)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary output. Error: %v\n", err)
		}
	} else {
		dataToDisplay, err = a.AvroDecode(rec.Value)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if a.KeyProtoType != "" {
		keyToDisplay, err = ProtoDecode(a.Reg, rec.Key, a.KeyProtoType)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto key. falling back to binary output. Error: %v\n", err)
		}
	} else {
		keyToDisplay, err = a.AvroDecode(rec.Key)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if a.DecodeMsgPack {
		var obj any
		err = msgpack.Unmarshal(rec.Value, &obj)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode msgpack data: %v\n", err)
		}

		dataToDisplay, err = json.Marshal(obj)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode msgpack data: %v\n", err)
		}
	}

	dataToDisplay = a.FormatMessage(rec, dataToDisplay, keyToDisplay, &stderr, outputFmt)

	mu.Lock()
	stderr.WriteTo(a.ErrWriter)
	_, _ = a.ColorableOut.Write(dataToDisplay)
	fmt.Fprintln(a.OutWriter)
	mu.Unlock()
}

// FormatMessage renders a record according to the output format.
func (a *App) FormatMessage(rec *kgo.Record, rawMessage []byte, keyToDisplay []byte, stderr *bytes.Buffer, outputFmt OutputFormat) []byte {
	switch outputFmt {
	case OutputFormatRaw:
		return rawMessage
	case OutputFormatJSON:
		jsonMessage := make(map[string]any)
		jsonMessage["partition"] = rec.Partition
		jsonMessage["offset"] = rec.Offset
		jsonMessage["timestamp"] = rec.Timestamp
		if len(rec.Headers) > 0 {
			jsonMessage["headers"] = rec.Headers
		}
		jsonMessage["key"] = FormatJSON(keyToDisplay)
		jsonMessage["payload"] = FormatJSON(rawMessage)

		jsonToDisplay, err := json.Marshal(jsonMessage)
		if err != nil {
			fmt.Fprintf(stderr, "could not decode JSON data: %v", err)
		}
		return jsonToDisplay
	case OutputFormatJSONEachRow:
		jsonMessage := JSONEachRowMessage{
			Topic:     rec.Topic,
			Partition: rec.Partition,
			Offset:    rec.Offset,
			Timestamp: rec.Timestamp,
			Headers:   make([]MessageHeader, len(rec.Headers)),
			Key:       string(keyToDisplay),
			Payload:   string(rawMessage),
		}
		for i, hdr := range rec.Headers {
			jsonMessage.Headers[i] = MessageHeader{
				Key:   hdr.Key,
				Value: ParseHeader(hdr.Value),
			}
		}
		jsonToDisplay, err := json.Marshal(jsonMessage)
		if err != nil {
			fmt.Fprintf(stderr, "could not decode JSON data: %v", err)
		}
		return jsonToDisplay
	case OutputFormatHex:
		return []byte(hex.EncodeToString(rawMessage))
	default:
		if IsJSON(rawMessage) {
			rawMessage = FormatValue(rawMessage)
		}
		if IsJSON(keyToDisplay) {
			keyToDisplay = a.FormatKey(keyToDisplay)
		}

		w := NewTabWriter(stderr)
		if len(rec.Headers) > 0 {
			fmt.Fprintf(w, "Headers:\n")
		}
		for _, hdr := range rec.Headers {
			fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", hdr.Key, ParseHeader(hdr.Value))
		}
		if len(rec.Key) > 0 {
			fmt.Fprintf(w, "Key:\t%v\n", string(keyToDisplay))
		}
		fmt.Fprintf(w, "Partition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", rec.Partition, rec.Offset, rec.Timestamp)
		w.Flush()

		return rawMessage
	}
}

// FormatKey pretty-prints a JSON key using the compact formatter.
func (a *App) FormatKey(key []byte) []byte {
	if b, err := a.Keyfmt.Format(key); err == nil {
		return b
	}
	return key
}

// AvroDecode attempts Avro decoding if a schema cache is set.
func (a *App) AvroDecode(b []byte) ([]byte, error) {
	if a.SchemaCache != nil {
		return a.SchemaCache.DecodeMessage(b)
	}
	return b, nil
}

// ProtoDecode decodes protobuf bytes to JSON using the given registry.
func ProtoDecode(reg *proto.DescriptorRegistry, b []byte, _type string) ([]byte, error) {
	dynamicMessage := reg.MessageForType(_type)
	if dynamicMessage == nil {
		return b, nil
	}

	if err := goproto.Unmarshal(b, dynamicMessage); err != nil {
		return nil, err
	}

	jsonBytes, err := protojson.Marshal(dynamicMessage)
	if err != nil {
		return nil, err
	}
	return jsonBytes, nil
}

// ProtoEncode encodes JSON input to protobuf bytes using the given registry.
func (a *App) ProtoEncode(input []byte, msgType string) ([]byte, error) {
	dynamicMessage := a.Reg.MessageForType(msgType)
	if dynamicMessage == nil {
		return nil, fmt.Errorf("proto type %v not found", msgType)
	}
	if err := protojson.Unmarshal(input, dynamicMessage); err != nil {
		return nil, fmt.Errorf("parse input JSON as proto type %v: %w", msgType, err)
	}
	pbBytes, err := goproto.Marshal(dynamicMessage)
	if err != nil {
		return nil, fmt.Errorf("marshal proto: %w", err)
	}
	return pbBytes, nil
}

// ParseHeader tries to decode azure eventhub-specific header encoding,
// falling back to raw string.
func ParseHeader(hdrBytes []byte) string {
	if len(hdrBytes) == 0 {
		return ""
	}
	switch hdrBytes[0] {
	case 161:
		if len(hdrBytes) < 2 {
			return string(hdrBytes)
		}
		end := 2 + int(hdrBytes[1])
		if end > len(hdrBytes) {
			return string(hdrBytes)
		}
		return string(hdrBytes[2:end])
	case 131:
		if len(hdrBytes) < 9 {
			return string(hdrBytes)
		}
		return strconv.FormatUint(binary.BigEndian.Uint64(hdrBytes[1:9]), 10)
	default:
		return string(hdrBytes)
	}
}

// CheckHeaders returns true if rec headers match all filter entries.
func CheckHeaders(headers []kgo.RecordHeader, filter map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	matchCount := 0
	for _, h := range headers {
		if val, ok := filter[h.Key]; ok && ParseHeader(h.Value) == val {
			matchCount++
		}
	}
	return matchCount >= len(filter)
}

// EndOffsetMapForTopic extracts per-partition end offsets for a single topic.
func EndOffsetMapForTopic(offsets kadm.ListedOffsets, topic string) map[int32]int64 {
	m := make(map[int32]int64)
	offsets.Each(func(lo kadm.ListedOffset) {
		if lo.Topic == topic {
			m[lo.Partition] = lo.Offset
		}
	})
	return m
}
