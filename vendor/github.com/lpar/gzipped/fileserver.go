package gzipped

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/golang/gddo/httputil/header"
)

const (
	gzipEncoding  = "gzip"
	gzipExtension = ".gz"

	brotliEncoding  = "br"
	brotliExtension = ".br"
)

type fileHandler struct {
	root http.FileSystem
}

// FileServer is a drop-in replacement for Go's standard http.FileServer
// which adds support for static resources precompressed with gzip, at
// the cost of removing the support for directory browsing.
//
// If file filename.ext has a compressed version filename.ext.gz alongside
// it, if the client indicates that it accepts gzip-compressed data, and
// if the .gz file can be opened, then the compressed version of the file
// will be sent to the client. Otherwise the request is passed on to
// http.ServeContent, and the raw (uncompressed) version is used.
//
// It is up to you to ensure that the compressed and uncompressed versions
// of files match and have sensible timestamps.
//
// Compressed or not, requests are fulfilled using http.ServeContent, and
// details like accept ranges and content-type sniffing are handled by that
// method.
func FileServer(root http.FileSystem) http.Handler {
	return &fileHandler{root}
}

func acceptable(r *http.Request, encoding string) bool {
	for _, aspec := range header.ParseAccept(r.Header, "Accept-Encoding") {
		if aspec.Value == encoding && aspec.Q == 0.0 {
			return false
		}
		if (aspec.Value == encoding || aspec.Value == "*") && aspec.Q > 0.0 {
			return true
		}
	}
	return false
}

func (f *fileHandler) openAndStat(path string) (http.File, os.FileInfo, error) {
	file, err := f.root.Open(path)
	var info os.FileInfo
	// This slightly weird variable reuse is so we can get 100% test coverage
	// without having to come up with a test file that can be opened, yet
	// fails to stat.
	if err == nil {
		info, err = file.Stat()
	}
	if err != nil {
		return file, nil, err
	}
	if info.IsDir() {
		return file, nil, fmt.Errorf("%s is directory", path)
	}
	return file, info, nil
}

func (f *fileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upath := r.URL.Path
	if !strings.HasPrefix(upath, "/") {
		upath = "/" + upath
		r.URL.Path = upath
	}
	fpath := path.Clean(upath)
	if strings.HasSuffix(fpath, "/") {
		// If you wanted to put back directory browsing support, this is
		// where you'd do it.
		http.NotFound(w, r)
		return
	}
	// Try for a compressed version if appropriate
	var file http.File
	var err error
	var info os.FileInfo

	foundAcceptable := false

	if acceptable(r, brotliEncoding) {
		file, info, err = f.openAndStat(fpath + brotliExtension)
		if err == nil {
			foundAcceptable = true
			w.Header().Set("Content-Encoding", brotliEncoding)
		}

	}

	if !foundAcceptable && acceptable(r, gzipEncoding) {
		file, info, err = f.openAndStat(fpath + gzipExtension)
		if err == nil {
			foundAcceptable = true
			w.Header().Set("Content-Encoding", gzipEncoding)
		}
	}
	// If we didn't manage to open a compressed version, try for uncompressed
	if !foundAcceptable {
		file, info, err = f.openAndStat(fpath)
	}
	if err != nil {
		// Doesn't exist compressed or uncompressed
		http.NotFound(w, r)
		return
	}
	defer file.Close()
	http.ServeContent(w, r, fpath, info.ModTime(), file)
}
