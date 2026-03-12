package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"golang.org/x/sync/errgroup"
)

// Manager starts registered Runnables in order, and shuts them down when a signal is received.
type Manager interface {
	// Register adds a Runnable to the Manager.
	Register(Runnable)
	// Run starts all registered Runnables in order by calling their Run method.
	// If any Runnable's Run method returns an error , the Manager initiates a shutdown.
	Run() error
	// Shutdown calls the Shutdown method on all Runnables in reverse order of registration.
	// It will wait for each Runnable to shut down before moving on to the next.
	Shutdown() error
}

func NewManager() Manager {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)

	return &manager{
		runnables: make([]Runnable, 0),
		sigint:    sigint,
	}
}

type manager struct {
	runnables []Runnable
	sigint    chan os.Signal
}

func (m *manager) Run() error {
	g, ctx := errgroup.WithContext(context.Background())
	for _, r := range m.runnables {
		g.Go(func() error {
			return r.Run()
		})
	}

	select {
	case <-ctx.Done():
		return errors.Join(
			context.Cause(ctx),
			m.Shutdown(),
		)
	case <-m.sigint:
		return m.Shutdown()
	}
}

func (m *manager) Shutdown() error {
	errs := make([]error, 0)
	for i := len(m.runnables) - 1; i >= 0; i-- {
		err := m.runnables[i].Shutdown()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("encountered errors shutting down: \n%w", errors.Join(errs...))
	}
	return nil
}

func (m *manager) Register(r Runnable) {
	m.runnables = append(m.runnables, r)
}
