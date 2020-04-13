//+build generate

package main

import (
	"log"
	"net/http"

	"os"
	"time"

	"github.com/shurcooL/vfsgen"
)

//go:generate bash -c "npm i && npm run build -- --spa"
//go:generate bash -c "go run gen.go"

func main() {
	var fs http.FileSystem = http.Dir("./client/dist/")

	err := vfsgen.Generate(modTimeFS{fs: fs}, vfsgen.Options{
		PackageName:  "client",
		VariableName: "Assets",
	})
	if err != nil {
		log.Fatalln(err)
	}
}

// modTimeFS is an http.FileSystem wrapper that modifies
// underlying fs such that all of its file mod times are set to zero.
// We do this to not have a diff every time we re-generate files, even if they are the same.
type modTimeFS struct {
	fs http.FileSystem
}

func (fs modTimeFS) Open(name string) (http.File, error) {
	f, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	return modTimeFile{f}, nil
}

type modTimeFile struct {
	http.File
}

func (f modTimeFile) Stat() (os.FileInfo, error) {
	fi, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	return modTimeFileInfo{fi}, nil
}

type modTimeFileInfo struct {
	os.FileInfo
}

func (modTimeFileInfo) ModTime() time.Time {
	return time.Time{}
	// or any custom logic you'd like to implement for your needs
	// (perhaps use the modtime of the protobuf files you're generating from)
}
