package main

import (
	"net"

	"github.com/birdayz/kaf/pkg/topic"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func init() {
	rootCmd.AddCommand(serveCmd)
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start server",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		var svc topic.Service

		l, err := net.Listen("tcp", ":8080")
		if err != nil {
			errorExit("Failed to listen")
		}
		srv := grpc.NewServer()
		reflection.Register(srv)
		topic.RegisterTopicServiceServer(srv, &svc)

		err = srv.Serve(l)
	},
}
