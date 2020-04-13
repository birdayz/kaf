package main

import (
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/birdayz/kaf/api"
	"github.com/birdayz/kaf/client"
	"github.com/birdayz/kaf/pkg/topic"
	"github.com/gorilla/websocket"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/lpar/gzipped"
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

		srv := grpc.NewServer()

		lnr, err := net.Listen("tcp", ":8080")
		if err != nil {
			errorExit("Failed to listen")
		}

		reflection.Register(srv)
		api.RegisterTopicServiceServer(srv, &svc)

		wrappedServer := grpcweb.WrapServer(srv, grpcweb.WithWebsockets(true))

		httpSrv := &http.Server{
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       120 * time.Second,
			Addr:              "localhost:8081",
			Handler: grpcTrafficSplitter(
				folderReader(
					gzipped.FileServer(client.Assets).ServeHTTP,
				),
				wrappedServer,
			),
		}

		httpSrv.ListenAndServe()
		srv.Serve(lnr)
	},
}

func folderReader(fn http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/") {
			// Use contents of index.html for directory, if present.
			r.URL.Path = path.Join(r.URL.Path, "index.html")
		}
		fn(w, r)
	})
}

func grpcTrafficSplitter(fallback http.HandlerFunc, grpcHandler http.Handler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Redirect gRPC and gRPC-Web requests to the gRPC Server
		if strings.Contains(r.Header.Get("Content-Type"), "application/grpc") ||
			websocket.IsWebSocketUpgrade(r) {
			grpcHandler.ServeHTTP(w, r)
		} else {

			fallback(w, r)
		}
	})
}
