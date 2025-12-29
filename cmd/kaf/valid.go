package main

import (
	"context"
	"github.com/spf13/cobra"
)

func validConfigArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	clusterList := make([]string, 0, len(cfg.Clusters))
	for _, cluster := range cfg.Clusters {
		clusterList = append(clusterList, cluster.Name)
	}
	return clusterList, cobra.ShellCompDirectiveNoFileComp
}

func validGroupArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	admin := getClusterAdmin()
	defer admin.Close()

	ctx := context.Background()
	groups, err := admin.ListGroups(ctx)
	if err != nil {
		errorExit("Unable to list consumer groups: %v\n", err)
	}
	groupNames := groups.Groups()
	groupList := make([]string, 0, len(groupNames))
	for _, grp := range groupNames {
		groupList = append(groupList, grp)
	}
	return groupList, cobra.ShellCompDirectiveNoFileComp
}

func validTopicArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	admin := getClusterAdmin()
	defer admin.Close()

	ctx := context.Background()
	topics, err := admin.ListTopics(ctx)
	if err != nil {
		errorExit("Unable to list topics: %v\n", err)
	}
	topicNames := topics.Names()
	topicList := make([]string, 0, len(topicNames))
	for _, topic := range topicNames {
		topicList = append(topicList, topic)
	}
	return topicList, cobra.ShellCompDirectiveNoFileComp
}
