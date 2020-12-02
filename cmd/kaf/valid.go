package main

import (
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

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		errorExit("Unable to list consumer groups: %v\n", err)
	}
	groupList := make([]string, 0, len(groups))
	for grp := range groups {
		groupList = append(groupList, grp)
	}
	return groupList, cobra.ShellCompDirectiveNoFileComp
}

func validTopicArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	admin := getClusterAdmin()

	topics, err := admin.ListTopics()
	if err != nil {
		errorExit("Unable to list topics: %v\n", err)
	}
	topicList := make([]string, 0, len(topics))
	for topic := range topics {
		topicList = append(topicList, topic)
	}
	return topicList, cobra.ShellCompDirectiveNoFileComp
}
