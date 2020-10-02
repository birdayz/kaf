// Package gnomock contains a framework to set up temporary docker containers
// for integration and end-to-end testing of other applications. It handles
// pulling images, starting containers, waiting for them to become available,
// setting up their initial state and cleaning up in the end.
//
// It can be used either directly, or via already existing implementations of
// various connectors built by the community.
package gnomock

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const defaultTag = "latest"

// newG creates a new Gnomock session with a unique identifier and a dedicated
// logger. It allows to follow a specific action while having multiple
// operations running in parallel.
func newG(debug bool) (*g, error) {
	if !debug {
		return &g{log: zap.NewNop().Sugar()}, nil
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("can't generate a unique session id")
	}

	cfg := zap.NewDevelopmentConfig()
	cfg.Encoding = "json"

	l, err := cfg.Build(zap.WithCaller(false))
	if err != nil {
		return nil, fmt.Errorf("can't setup logger")
	}

	l = l.With(zap.String("id", id.String()))

	return &g{id, l.Sugar()}, nil
}

// g is a Gnomock operation wrapper, mostly for debug purposes.
type g struct {
	id  uuid.UUID
	log *zap.SugaredLogger
}

// StartCustom creates a new container using provided image and binds random
// ports on the host to the provided ports inside the container. Image may
// include tag, which is set to "latest" by default. Optional configuration is
// available through Option functions. The returned container must be stopped
// when no longer needed using its Stop() method
func StartCustom(image string, ports NamedPorts, opts ...Option) (c *Container, err error) {
	config, image := buildConfig(opts...), buildImage(image)

	g, err := newG(config.Debug)
	if err != nil {
		return nil, fmt.Errorf("can't create new gnomock session: %w", err)
	}

	defer func() { _ = g.log.Sync() }()

	g.log.Infow("starting", "image", image, "ports", ports)
	g.log.Infow("using config", "image", image, "ports", ports, "config", config)

	ctx, cancel := context.WithTimeout(config.ctx, config.Timeout)
	defer cancel()

	cli, err := g.dockerConnect()
	if err != nil {
		return nil, fmt.Errorf("can't create docker client: %w", err)
	}

	err = cli.pullImage(ctx, image)
	if err != nil {
		return nil, fmt.Errorf("can't pull image: %w", err)
	}

	c, err = cli.startContainer(ctx, image, ports, config)
	if err != nil {
		return nil, fmt.Errorf("can't start container: %w", err)
	}

	defer func() {
		if err != nil {
			if !config.Debug && Stop(c) == nil {
				c = nil
			}
		}
	}()

	err = g.setupLogForwarding(c, cli, config)
	if err != nil {
		return nil, fmt.Errorf("can't setup log forwarding: %w", err)
	}

	err = g.wait(ctx, c, config)
	if err != nil {
		return c, fmt.Errorf("can't connect to container: %w", err)
	}

	err = g.initf(ctx, c, config)
	if err != nil {
		return c, fmt.Errorf("can't init container: %w", err)
	}

	g.log.Infow("container is ready to use", "id", c.ID, "ports", c.Ports)

	return c, nil
}

func copy(dst io.Writer, src io.Reader) func() error {
	return func() error {
		_, err := stdcopy.StdCopy(dst, dst, src)
		if err != nil && err != io.EOF {
			return err
		}

		return nil
	}
}

func closeLogReader(logReader io.ReadCloser, g *errgroup.Group) func() error {
	return func() error {
		err := logReader.Close()
		if err != nil {
			return err
		}

		err = g.Wait()
		if err != nil {
			return err
		}

		return nil
	}
}

// Start creates a container using the provided Preset. The Preset provides its
// own Options to configure Gnomock container. Usually this is enough, but it
// is still possible to extend/override Preset options with new values. For
// example, wait timeout defined in the Preset, if at all, might be not enough
// for this particular usage, so it can't be changed during this call.
//
// All provided Options are applied. First, Preset options are applied. Then,
// custom Options. If both Preset and custom Options change the same
// configuration, custom Options are used
func Start(p Preset, opts ...Option) (*Container, error) {
	presetOpts := p.Options()

	mergedOpts := make([]Option, 0, len(opts)+len(presetOpts))
	mergedOpts = append(mergedOpts, presetOpts...)
	mergedOpts = append(mergedOpts, opts...)

	return StartCustom(p.Image(), p.Ports(), mergedOpts...)
}

// Stop stops the provided container and lets docker remove them from the
// system. Stop returns an error if any one of the containers couldn't stop
func Stop(cs ...*Container) error {
	g, err := newG(isInDocker())
	if err != nil {
		return err
	}

	defer func() { _ = g.log.Sync() }()

	var eg errgroup.Group

	for _, c := range cs {
		container := c

		eg.Go(func() error {
			return g.stop(container)
		})
	}

	return eg.Wait()
}

func (g *g) stop(c *Container) error {
	if c == nil {
		return nil
	}

	g.log.Infow("stopping", "container", c)

	cli, err := g.dockerConnect()
	if err != nil {
		return fmt.Errorf("can't create docker client: %w", err)
	}

	err = cli.stopContainer(context.Background(), c.ID)
	if err != nil {
		return fmt.Errorf("can't stop container: %w", err)
	}

	if c.onStop != nil {
		err = c.onStop()
		if err != nil {
			return fmt.Errorf("can't perform last cleanup: %w", err)
		}
	}

	return nil
}

func buildImage(image string) string {
	parts := strings.Split(image, ":")

	noTagSet := len(parts) == 1
	if noTagSet {
		image = fmt.Sprintf("%s:%s", parts[0], defaultTag)
	}

	return image
}

func (g *g) setupLogForwarding(c *Container, cli *docker, config *Options) error {
	logReader, err := cli.readLogs(context.Background(), c.ID)
	if err != nil {
		return fmt.Errorf("can't create log reader: %w", err)
	}

	eg := &errgroup.Group{}
	eg.Go(copy(config.logWriter, logReader))
	c.onStop = closeLogReader(logReader, eg)

	return nil
}

func (g *g) wait(ctx context.Context, c *Container, config *Options) error {
	g.log.Info("waiting for healthcheck to pass")

	delay := time.NewTicker(config.healthcheckInterval)
	defer delay.Stop()

	var lastErr error

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("canceled after error: %w", lastErr)
		case <-delay.C:
			err := config.healthcheck(ctx, envAwareClone(c))
			if err == nil {
				g.log.Info("container is healthy")
				return nil
			}

			lastErr = err
		}
	}
}

func (g *g) initf(ctx context.Context, c *Container, config *Options) error {
	g.log.Info("starting initial state setup")

	return config.init(ctx, envAwareClone(c))
}

// envAwareClone returns a copy of the provided container adjusted for usage
// inside current environment. For example, if current process runs directly on
// the host where container ports are exposed, an exact copy will be returned.
// For a process running itself inside a container, Host value will be replaced
// by docker host IP address. Anyway, calling Address() on the returned
// container will allow to communicate with it both from inside another
// container or from docker host.
func envAwareClone(c *Container) *Container {
	containerCopy := &Container{
		ID:    c.ID,
		Host:  c.Host,
		Ports: c.Ports,
	}

	// when gnomock runs inside docker container, the other container is only
	// accessible through the host
	if isInDocker() {
		containerCopy.Host = c.gateway
	}

	return containerCopy
}
