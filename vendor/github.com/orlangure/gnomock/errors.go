package gnomock

import "fmt"

// ErrEnvClient means that Gnomock can't connect to docker daemon in the
// testing environment. See https://docs.docker.com/compose/reference/overview/
// for information on required configuration
var ErrEnvClient = fmt.Errorf("can't connect to docker host")
