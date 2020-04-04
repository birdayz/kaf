# gzipped.FileServer

Drop-in replacement for golang http.FileServer which supports gzipped static 
content.

This allows major bandwidth savings for CSS, JavaScript libraries, fonts, and
other static compressible web content. It also means you can compress the
content with zopfli without significant runtime penalty.

## Example

Suppose `/var/www/assets/css` contains your style sheets, and you want to make them available as `/css/*.css`:

    package main
    
    import (
    	"log"
    	"net/http"
    
    	"github.com/lpar/gzipped"
    )
    
    func main() {
    	log.Fatal(http.ListenAndServe(":8080", http.StripPrefix("/css",
        gzipped.FileServer(http.Dir("/var/www/assets/css")))))
    }
    // curl localhost:8080/css/styles.css


Using [httprouter](https://github.com/julienschmidt/httprouter)?

    router := httprouter.New()
    router.Handler("GET", "/css/*filepath", 
      gzipped.FileServer(http.Dir("/var/www/assets/css"))))
    log.Fatal(http.ListenAndServe(":8080", router)

## Detail

For any given request at `/path/filename.ext`, if:

  1. the client will accept gzipped content, and
  2. there exists a file named `/path/filename.ext.gz` (starting from the 
     appropriate base directory), and
  3. the file can be opened,

then the compressed file will be served as `/path/filename.ext`, with a
`Content-Encoding` header set so that the client transparently decompresses it.
Otherwise, the request is passed through and handled unchanged.

Unlike other similar code I found, this package has a license, parses 
Accept-Encoding headers properly, and has unit tests.

## Caveats

All requests are passed to Go's standard `http.ServeContent` method for
fulfilment. MIME type sniffing, accept ranges, content negotiation and other
tricky details are handled by that method.

It is up to you to ensure that your compressed and uncompressed resources are
kept in sync.

Directory browsing isn't supported. (You probably don't want it on your
application anyway, and if you do then you probably don't want Go's default
implementation.)

## Related

 * You might consider precompressing your CSS with [minify](https://github.com/tdewolff/minify). 

 * If you want to get the best possible compression, use [zopfli](https://github.com/google/zopfli).

 * To compress your dynamically-generated HTML pages on the fly, I suggest [gziphandler](https://github.com/NYTimes/gziphandler).

