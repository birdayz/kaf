//+build generate

package main

import (
	"log"
	"net/http"

	"github.com/shurcooL/vfsgen"
)

//go:generate bash -c "go run gen.go"

func main() {
	var fs http.FileSystem = http.Dir("./html/")

	err := vfsgen.Generate(fs, vfsgen.Options{
		PackageName:  "client",
		VariableName: "Assets",
	})
	if err != nil {
		log.Fatalln(err)
	}
}
