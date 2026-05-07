package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"golang.org/x/sync/errgroup"
)

func NewManager() *Manager {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)

	return &Manager{
		runnables: make([]Runnable, 0),
		sigint:    sigint,
	}
}

type Manager struct {
	runnables []Runnable
	sigint    chan os.Signal
}

// Register adds a Runnable to the Manager.
func (m *Manager) Register(r Runnable) {
	m.runnables = append(m.runnables, r)
}

// Run starts all registered Runnables in order by calling their Run method.
// If any Runnable's Run method returns an error , the Manager initiates a shutdown.
func (m *Manager) Run() error {
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

// Shutdown calls the Shutdown method on all Runnables in reverse order of registration.
// It will wait for each Runnable to shut down before moving on to the next.
func (m *Manager) Shutdown() error {
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
