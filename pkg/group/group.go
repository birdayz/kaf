package group

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
)

// GroupInfo holds summary info for a consumer group listing.
type GroupInfo struct {
	Name      string
	State     string
	Consumers int
}

// List returns all consumer groups with state and member count.
func List(ctx context.Context, admin *kadm.Client) ([]GroupInfo, error) {
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(listed))
	for name := range listed {
		names = append(names, name)
	}
	sort.Strings(names)

	described, err := admin.DescribeGroups(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("describe groups: %w", err)
	}

	result := make([]GroupInfo, 0, len(described))
	for _, g := range described.Sorted() {
		result = append(result, GroupInfo{
			Name:      g.Group,
			State:     g.State,
			Consumers: len(g.Members),
		})
	}
	return result, nil
}

// MemberInfo holds a single member's details.
type MemberInfo struct {
	ClientID   string
	ClientHost string
	// TopicPartitions maps topic -> assigned partitions.
	TopicPartitions map[string][]int32
}

// PartitionOffset holds per-partition offset and lag info.
type PartitionOffset struct {
	Partition     int32
	GroupOffset   int64
	HighWatermark int64
	Lag           int64
	Metadata      string
}

// TopicOffsets groups partition offsets per topic, plus totals.
type TopicOffsets struct {
	Topic      string
	Partitions []PartitionOffset
	TotalLag   int64
	TotalOffset int64
}

// GroupDescription holds the full description of a consumer group.
type GroupDescription struct {
	Group        string
	State        string
	Protocol     string
	ProtocolType string
	Topics       []TopicOffsets
	Members      []MemberInfo
}

// Describe returns a full description of a consumer group including offsets and lag.
func Describe(ctx context.Context, admin *kadm.Client, group string, filterTopics []string) (*GroupDescription, error) {
	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		return nil, err
	}

	g, ok := described[group]
	if !ok {
		return nil, fmt.Errorf("group %s not found", group)
	}
	if g.Err != nil {
		return nil, g.Err
	}
	if g.State == "Dead" {
		return nil, fmt.Errorf("group %s not found", group)
	}

	// Fetch committed offsets for all topics.
	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets: %w", err)
	}

	// Collect topics from the offsets.
	topicNames := make([]string, 0)
	topicPartitions := make(map[string][]int32)
	offsets.Each(func(o kadm.OffsetResponse) {
		topicPartitions[o.Topic] = append(topicPartitions[o.Topic], o.Partition)
	})
	for t := range topicPartitions {
		topicNames = append(topicNames, t)
	}
	sort.Strings(topicNames)

	// Get high watermarks for these topics.
	endOffsets, err := admin.ListEndOffsets(ctx, topicNames...)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}

	filterSet := make(map[string]bool, len(filterTopics))
	for _, t := range filterTopics {
		filterSet[t] = true
	}

	var topics []TopicOffsets
	for _, topicName := range topicNames {
		if len(filterSet) > 0 && !filterSet[topicName] {
			continue
		}

		parts := topicPartitions[topicName]
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })

		var to TopicOffsets
		to.Topic = topicName

		for _, p := range parts {
			o, _ := offsets.Lookup(topicName, p)
			var hwm int64
			if eo, ok := endOffsets.Lookup(topicName, p); ok {
				hwm = eo.Offset
			}
			lag := hwm - o.Offset.At
			if lag < 0 {
				lag = 0
			}
			to.Partitions = append(to.Partitions, PartitionOffset{
				Partition:     p,
				GroupOffset:   o.Offset.At,
				HighWatermark: hwm,
				Lag:           lag,
				Metadata:      o.Offset.Metadata,
			})
			to.TotalLag += lag
			to.TotalOffset += o.Offset.At
		}
		topics = append(topics, to)
	}

	// Members.
	var members []MemberInfo
	for _, m := range g.Members {
		mi := MemberInfo{
			ClientID:        m.ClientID,
			ClientHost:      m.ClientHost,
			TopicPartitions: make(map[string][]int32),
		}
		if ca, ok := m.Assigned.AsConsumer(); ok {
			for _, t := range ca.Topics {
				mi.TopicPartitions[t.Topic] = t.Partitions
			}
		}
		members = append(members, mi)
	}

	return &GroupDescription{
		Group:        g.Group,
		State:        g.State,
		Protocol:     g.Protocol,
		ProtocolType: g.ProtocolType,
		Topics:       topics,
		Members:      members,
	}, nil
}

// Delete deletes a consumer group.
func Delete(ctx context.Context, admin *kadm.Client, group string) error {
	resp, err := admin.DeleteGroup(ctx, group)
	if err != nil {
		return err
	}
	return resp.Err
}

// CommitOffsets commits offsets for a topic in a consumer group.
func CommitOffsets(ctx context.Context, admin *kadm.Client, group string, topicOffsets map[string]map[int32]int64) error {
	os := make(kadm.Offsets)
	for topic, partitions := range topicOffsets {
		for partition, offset := range partitions {
			os.Add(kadm.Offset{
				Topic:     topic,
				Partition: partition,
				At:        offset,
			})
		}
	}
	return admin.CommitAllOffsets(ctx, group, os)
}

// ListGroupNames returns just the group names (for shell completion).
func ListGroupNames(ctx context.Context, admin *kadm.Client) ([]string, error) {
	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(listed))
	for name := range listed {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
