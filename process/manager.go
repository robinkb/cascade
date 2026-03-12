package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"golang.org/x/sync/errgroup"
)

/*
 * What do I actually need this to do?
 *
 * 	1. Start Go routines
 * 		1. Report errors, if any
 * 		2. Order doesn't matter, at least not now
 *      3. What do if a Go routine fails?
 * 		-> Shut them all down (could be improved later)
 * 		-> Exit with non-zero exit code
 *  2. Stop Go routines
 * 		1. Report errors, if any
 * 		2. Order doesn't matter, at least not now
 * 		3. What do if a Go routine fails?
 * 		-> Exit with non-zero exit code
 *
 * What do I _not_ need this to do (for now)?
 *
 * 	1. Restart Go routines if they fail
 */
type Manager interface {
	Register(Runnable)
	Run() error
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
		return g.Wait()
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
