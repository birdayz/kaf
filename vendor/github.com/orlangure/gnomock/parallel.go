package gnomock

import "golang.org/x/sync/errgroup"

// InParallel begins parallel preset execution setup. Use Start to add more
// presets with their configuration to parallel execution, and Go() in the end
// to kick-off everything
func InParallel() *Parallel {
	return &Parallel{}
}

type configuredPreset struct {
	Preset

	opts []Option
}

// Parallel is a builder object that configures parallel preset execution
type Parallel struct {
	presets []configuredPreset
}

// Start adds the provided preset with its configuration to the parallel
// execution kicked-off by Go(), together with other added presets
func (b *Parallel) Start(p Preset, opts ...Option) *Parallel {
	b.presets = append(b.presets, configuredPreset{p, opts})

	return b
}

// Go kicks-off parallel preset execution. Returned containers are in the same
// order as they were added with Start. An error is returned if any of the
// containers failed to start and become available. Even if Go() returns an
// errors, there still might be containers created in the process, and it is
// callers responsibility to Stop them
func (b *Parallel) Go() ([]*Container, error) {
	var g errgroup.Group

	containers := make([]*Container, len(b.presets))

	for i, preset := range b.presets {
		containerIndex := i
		p := preset

		g.Go(func() error {
			c, err := Start(p.Preset, p.opts...)
			containers[containerIndex] = c

			return err
		})
	}

	return containers, g.Wait()
}
