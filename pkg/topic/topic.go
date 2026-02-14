package topic

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TopicInfo represents a topic listing entry.
type TopicInfo struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
}

// List returns all topics sorted by name.
func List(ctx context.Context, admin *kadm.Client) ([]TopicInfo, error) {
	topics, err := admin.ListTopics(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]TopicInfo, 0, len(topics))
	for _, t := range topics.Sorted() {
		var rf int16
		sorted := t.Partitions.Sorted()
		if len(sorted) > 0 {
			rf = int16(len(sorted[0].Replicas))
		}
		result = append(result, TopicInfo{
			Name:              t.Topic,
			Partitions:        int32(len(t.Partitions)),
			ReplicationFactor: rf,
		})
	}

	return result, nil
}

// PartitionInfo holds partition detail for topic description.
type PartitionInfo struct {
	ID            int32
	Leader        int32
	Replicas      []int32
	ISR           []int32
	HighWatermark int64
}

// TopicDescription holds the full description of a topic.
type TopicDescription struct {
	Name       string
	IsInternal bool
	Compacted  bool
	Partitions []PartitionInfo
	Configs    []ConfigEntry
}

// ConfigEntry holds a non-default config entry.
type ConfigEntry struct {
	Name      string
	Value     string
	ReadOnly  bool
	Sensitive bool
	IsDefault bool
}

// Describe returns full topic details including partitions and config.
func Describe(ctx context.Context, admin *kadm.Client, topic string) (*TopicDescription, error) {
	topics, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return nil, err
	}

	detail, ok := topics[topic]
	if !ok || detail.Err != nil {
		if ok && detail.Err != nil {
			return nil, detail.Err
		}
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	// Get high watermarks.
	endOffsets, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}

	// Get config.
	configs, err := admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("describe config: %w", err)
	}

	var compacted bool
	var cfgEntries []ConfigEntry
	if len(configs) > 0 {
		for _, c := range configs[0].Configs {
			if c.Key == "cleanup.policy" && c.Value != nil && *c.Value == "compact" {
				compacted = true
			}
			cfgEntries = append(cfgEntries, ConfigEntry{
				Name:      c.Key,
				Value:     derefStr(c.Value),
				Sensitive: c.Sensitive,
				IsDefault: c.Source == kmsg.ConfigSource(5), // DEFAULT_CONFIG
			})
		}
	}

	sorted := detail.Partitions.Sorted()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Partition < sorted[j].Partition })

	partitions := make([]PartitionInfo, 0, len(sorted))
	for _, p := range sorted {
		var hwm int64
		if o, ok := endOffsets.Lookup(topic, p.Partition); ok {
			hwm = o.Offset
		}
		replicas := make([]int32, len(p.Replicas))
		copy(replicas, p.Replicas)
		sort.Slice(replicas, func(i, j int) bool { return replicas[i] < replicas[j] })
		isr := make([]int32, len(p.ISR))
		copy(isr, p.ISR)
		sort.Slice(isr, func(i, j int) bool { return isr[i] < isr[j] })
		partitions = append(partitions, PartitionInfo{
			ID:            p.Partition,
			Leader:        p.Leader,
			Replicas:      replicas,
			ISR:           isr,
			HighWatermark: hwm,
		})
	}

	return &TopicDescription{
		Name:       detail.Topic,
		IsInternal: detail.IsInternal,
		Compacted:  compacted,
		Partitions: partitions,
		Configs:    cfgEntries,
	}, nil
}

// Create creates a topic with the given parameters.
func Create(ctx context.Context, admin *kadm.Client, name string, partitions int32, replicas int16, configs map[string]*string) error {
	resp, err := admin.CreateTopic(ctx, partitions, replicas, configs, name)
	if err != nil {
		return err
	}
	return resp.Err
}

// Delete deletes a topic.
func Delete(ctx context.Context, admin *kadm.Client, name string) error {
	resp, err := admin.DeleteTopic(ctx, name)
	if err != nil {
		return err
	}
	return resp.Err
}

// SetConfig sets topic configuration entries using incremental alter.
func SetConfig(ctx context.Context, admin *kadm.Client, topic string, configs map[string]string) error {
	alterConfigs := make([]kadm.AlterConfig, 0, len(configs))
	for k, v := range configs {
		v := v
		alterConfigs = append(alterConfigs, kadm.AlterConfig{
			Op:    kadm.SetConfig,
			Name:  k,
			Value: &v,
		})
	}

	resps, err := admin.AlterTopicConfigsState(ctx, alterConfigs, topic)
	if err != nil {
		return err
	}
	for _, r := range resps {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// UpdatePartitions sets the partition count on a topic.
func UpdatePartitions(ctx context.Context, admin *kadm.Client, topic string, count int) error {
	resps, err := admin.UpdatePartitions(ctx, count, topic)
	if err != nil {
		return err
	}
	for _, r := range resps.Sorted() {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
