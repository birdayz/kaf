package main

import (
	"bytes"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/birdayz/kaf/api"
	"github.com/birdayz/kaf/client"
	"github.com/birdayz/kaf/pkg/cluster"
	"github.com/birdayz/kaf/pkg/connection"
	"github.com/birdayz/kaf/pkg/topic"
	"github.com/gorilla/websocket"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/lpar/gzipped"
	"github.com/rs/cors"
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
		c := connection.NewConnManager()
		svc, err := topic.NewService(c)
		if err != nil {
			errorExit("Failed to create topic service: %v", err)
		}

		clusterSvc := cluster.NewService(c)

		srv := grpc.NewServer()

		lnr, err := net.Listen("tcp", ":8080")
		if err != nil {
			errorExit("Failed to listen")
		}

		reflection.Register(srv)

		api.RegisterTopicServiceServer(srv, svc)
		api.RegisterClusterServiceServer(srv, clusterSvc)

		wrappedServer := grpcweb.WrapServer(srv, grpcweb.WithWebsockets(true))

		httpSrv := &http.Server{
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       120 * time.Second,
			Addr:              "localhost:8081",
			Handler: cors.AllowAll().Handler(grpcTrafficSplitter(
				fallback(folderReader(
					gzipped.FileServer(client.Assets).ServeHTTP,
				)),
				wrappedServer,
			)),
		}

		go func() {
			srv.Serve(lnr)
		}()
		_ = httpSrv.ListenAndServe()
	},
}

type TempResponseWriter struct {
	path string
	w    http.ResponseWriter
	r    *http.Request

	hdr http.Header

	b bytes.Buffer

	statusCode int
}

func (t *TempResponseWriter) Header() http.Header {
	return t.hdr
}

func (t *TempResponseWriter) Write(b []byte) (int, error) {
	return t.b.Write(b)
}

func (t *TempResponseWriter) WriteHeader(statusCode int) {
	t.statusCode = statusCode
}

func fallback(fn http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write to temporary response writer. IF OK, forward
		// the response. Otherwise, issue another request to
		// "index.html" as fallback
		t := &TempResponseWriter{path: r.URL.Path, w: w, r: r, hdr: make(http.Header)}
		fn(t, r)
		if t.statusCode == 404 {
			// Default to index.html
			r.URL.Path = "index.html"
			fn(w, r)

		} else {
			for k, v := range t.hdr {
				for _, val := range v {
					w.Header().Set(k, val)
				}
			}

			w.WriteHeader(t.statusCode)
			w.Write(t.b.Bytes())
		}
	})
}

func folderReader(fn http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/") {
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
