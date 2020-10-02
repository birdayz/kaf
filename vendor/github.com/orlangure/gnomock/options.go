package gnomock

import (
	"context"
	"io"
	"io/ioutil"
	"time"
)

const defaultTimeout = time.Second * 300
const defaultHealthcheckInterval = time.Millisecond * 250

// Option is an optional Gnomock configuration. Functions implementing this
// signature may be combined to configure Gnomock containers for different use
// cases
type Option func(*Options)

// WithContext sets the provided context to be used for setting up a Gnomock
// container. Canceling this context will cause Start() to abort
func WithContext(ctx context.Context) Option {
	return func(o *Options) {
		o.ctx = ctx
	}
}

// WithInit lets the provided InitFunc to be called when a Gnomock container is
// created, but before Start() returns. Use this function to run arbitrary code
// on the new container before using it. It can be useful to bring the
// container to a certain state (e.g create SQL schema)
func WithInit(f InitFunc) Option {
	return func(o *Options) {
		o.init = f
	}
}

// WithHealthCheck allows to define a rule to consider a Gnomock container
// ready to use. For example, it can attempt to connect to this container, and
// return an error on any failure, or nil on success. This function is called
// repeatedly until the timeout is reached, or until a nil error is returned
func WithHealthCheck(f HealthcheckFunc) Option {
	return func(o *Options) {
		o.healthcheck = f
	}
}

// WithHealthCheckInterval defines an interval between two consecutive health
// check calls. This is a constant interval
func WithHealthCheckInterval(t time.Duration) Option {
	return func(o *Options) {
		o.healthcheckInterval = t
	}
}

// WithTimeout sets the amount of time to wait for a created container to
// become ready to use. All startup steps must complete before they time out:
// start, wait until healthy, init.
func WithTimeout(t time.Duration) Option {
	return func(o *Options) {
		o.Timeout = t
	}
}

// WithEnv adds environment variable to the container. For example,
// AWS_ACCESS_KEY_ID=FOOBARBAZ
func WithEnv(env string) Option {
	return func(o *Options) {
		o.Env = append(o.Env, env)
	}
}

// WithLogWriter sets the target where to write container logs. This can be
// useful for debugging
func WithLogWriter(w io.Writer) Option {
	return func(o *Options) {
		o.logWriter = w
	}
}

// WithDebugMode allows Gnomock to output internal messages for debug purposes.
// Containers created in debug mode will not be automatically removed on
// failure to setup their initial state. Containers still might be removed if
// they are shut down from the inside. Use WithLogWriter to see what happens
// inside.
func WithDebugMode() Option {
	return func(o *Options) {
		o.Debug = true
	}
}

// WithContainerName allows to give a specific name to a new container. If a
// container with the same name already exists, it is killed.
func WithContainerName(name string) Option {
	return func(o *Options) {
		o.ContainerName = name
	}
}

// WithOptions allows to provide an existing set of Options instead of using
// optional configuration.
//
// This way has its own limitations. For example, context or initialization
// functions cannot be set in this way
func WithOptions(options *Options) Option {
	return func(o *Options) {
		if options.Timeout > 0 {
			o.Timeout = options.Timeout
		}

		o.Env = append(o.Env, options.Env...)
		o.Debug = options.Debug
		o.ContainerName = options.ContainerName
	}
}

// HealthcheckFunc defines a function to be used to determine container health.
// It receives a host and a port, and returns an error if the container is not
// ready, or nil when the container can be used. One example of HealthcheckFunc
// would be an attempt to establish the same connection to the container that
// the application under test uses
type HealthcheckFunc func(context.Context, *Container) error

func nopHealthcheck(context.Context, *Container) error {
	return nil
}

// InitFunc defines a function to be called on a ready to use container to set
// up its initial state before running the tests. For example, InitFunc can
// take care of creating a SQL table and inserting test data into it
type InitFunc func(context.Context, *Container) error

func nopInit(context.Context, *Container) error {
	return nil
}

// Options includes Gnomock startup configuration. Functional options
// (WithSomething) should be used instead of directly initializing objects of
// this type whenever possible
type Options struct {
	// Timeout is an amount of time to wait before considering Start operation
	// as failed.
	Timeout time.Duration `json:"timeout"`

	// Env is a list of environment variable to inject into the container. Each
	// entry is in format ENV_VAR_NAME=value
	Env []string `json:"env"`

	// Debug flag allows Gnomock to be verbose about steps it takes
	Debug bool `json:"debug"`

	// ContainerName allows to use a specific name for a new container. In case
	// a container with the same name already exists, Gnomock kills it.
	ContainerName string `json:"container_name"`

	ctx                 context.Context
	init                InitFunc
	healthcheck         HealthcheckFunc
	healthcheckInterval time.Duration
	logWriter           io.Writer
}

func buildConfig(opts ...Option) *Options {
	config := &Options{
		ctx:                 context.Background(),
		init:                nopInit,
		healthcheck:         nopHealthcheck,
		healthcheckInterval: defaultHealthcheckInterval,
		Timeout:             defaultTimeout,
		logWriter:           ioutil.Discard,
	}

	for _, opt := range opts {
		opt(config)
	}

	return config
}
