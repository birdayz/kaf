package gnomock

import (
	"fmt"
	"os"
)

// Container represents a docker container created for testing. Host and Ports
// fields should be used to configure the connection to this container. ID
// matches the original docker container ID
type Container struct {
	// Container ID as set by docker
	ID string `json:"id,omitempty"`

	// Host name of bound ports
	//
	// Default: localhost
	Host string `json:"host,omitempty"`

	// A collections of ports exposed by this container. Each port has an alias
	// and an actual port number as exposed on the host
	Ports NamedPorts `json:"ports,omitempty"`

	gateway string
	onStop  func() error
}

// Address is a convenience function that returns host:port that can be used to
// connect to this container. If a container was created with DefaultTCP call,
// use DefaultPort as the name. Otherwise, use the name of one of the ports
// used during setup
func (c *Container) Address(name string) string {
	p := c.Ports.Get(name)
	return fmt.Sprintf("%s:%d", c.Host, p.Port)
}

// DefaultAddress return Address() with DefaultPort
func (c *Container) DefaultAddress() string {
	return c.Address(DefaultPort)
}

// Port is a convenience function that returns port number with the provided
// name
func (c *Container) Port(name string) int {
	return c.Ports.Get(name).Port
}

// DefaultPort returns Port() with DefaultPort
func (c *Container) DefaultPort() int {
	return c.Port(DefaultPort)
}

func isInDocker() bool {
	_, err := os.Stat("/.dockerenv")
	return err == nil
}
