package gnomock

import "errors"

// DefaultPort should be used with simple containers that expose only one TCP
// port. Use DefaultTCP function when creating a container, and use DefaultPort
// when calling Address()
const DefaultPort = "default"

// ErrPortNotFound means that a port with the requested name was not found in
// the created container. Make sure that the name used in Find method matches
// the name used in in NamedPorts. For default values, use DefaultTCP and
// DefaultPort
var ErrPortNotFound = errors.New("port not found")

// Port is a combination of port number and protocol that are exposed in a
// container
type Port struct {
	// Protocol of the exposed port (TCP/UDP).
	Protocol string `json:"protocol"`

	// Port number of the exposed port.
	Port int `json:"port"`

	// HostPort is an optional value to set mapped host port explicitly.
	HostPort int `json:"host_port"`
}

// DefaultTCP is a utility function to use with simple containers exposing a
// single TCP port. Use it to create default named port for the provided port
// number. Pass DefaultPort to Address() method to get the address of the
// default port
func DefaultTCP(port int) NamedPorts {
	return NamedPorts{
		DefaultPort: Port{Protocol: "tcp", Port: port},
	}
}

// TCP returns a Port with the provided number and "tcp" protocol. This is a
// utility function, it is equivalent to creating a Port explicitly
func TCP(port int) Port {
	return Port{Protocol: "tcp", Port: port}
}

// NamedPorts is a collection of ports exposed by a container, where every
// exposed port is given a name. Some examples of names are "web" or "api" for
// a container that exposes two separate ports: one for web access and another
// for API calls
type NamedPorts map[string]Port

// Get returns a port with the provided name. An empty value is returned if
// there are no ports with the given name
func (p NamedPorts) Get(name string) Port {
	return p[name]
}

// Find returns the name of a port with the provided protocol and number. Use
// this method to find out the name of an exposed ports, when port number and
// protocol are known
func (p NamedPorts) Find(proto string, portNum int) (string, error) {
	for name, port := range p {
		if proto == port.Protocol && portNum == port.Port {
			return name, nil
		}
	}

	return "", ErrPortNotFound
}
