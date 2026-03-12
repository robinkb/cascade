package process

import (
	"errors"
	"testing"
	"time"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestManager(t *testing.T) {
	t.Run("starts and shuts down all registered runnables", func(t *testing.T) {
		const count = 10
		mgr := NewManager()

		log := make(chan string, count)
		runnables := make([]*SpyRunnable, count)
		for i := range runnables {
			runnables[i] = NewSpyRunnable(log)
			mgr.Register(runnables[i])
		}

		go func() {
			err := mgr.Run()
			AssertNoError(t, err)
		}()

		time.Sleep(1 * time.Millisecond)

		for _, r := range runnables {
			AssertEqual(t, r.started, true)
		}

		err := mgr.Shutdown()
		AssertNoError(t, err)

		// Runnables must be stopped in reverse registration order
		// to enable graceful shutdown.
		for i := len(runnables) - 1; i >= 0; i-- {
			AssertEqual(t, runnables[i].stopped, true)
			AssertEqual(t, <-log, "stopped "+runnables[i].Name())
		}
	})

	t.Run("returns an error when a runnable fails to start", func(t *testing.T) {
		mgr := NewManager()
		want := errors.New("broken")

		mgr.Register(NewBrokenRunnable(want))

		got := mgr.Run()
		AssertErrorIs(t, got, want)
	})

	t.Run("returns an error when a runnable fails to shutdown", func(t *testing.T) {
		mgr := NewManager()
		want := errors.New("broken")

		mgr.Register(NewBrokenRunnable(want))

		got := mgr.Shutdown()
		AssertErrorIs(t, got, want)
	})

	t.Run("initiates a shutdown when a Runnable fails", func(t *testing.T) {
		mgr := NewManager()

		spy := NewSpyRunnable(nil)
		want := errors.New("broken")
		broken := NewBrokenRunnable(want)

		mgr.Register(spy)
		mgr.Register(broken)

		got := mgr.Run()
		AssertErrorIs(t, got, want)
		AssertEqual(t, spy.stopped, true)
	})
}

func NewSpyRunnable(log chan<- string) *SpyRunnable {
	return &SpyRunnable{
		name: RandomString(8),
		done: make(chan struct{}),
		log:  log,
	}
}

type SpyRunnable struct {
	name    string
	started bool
	stopped bool
	done    chan struct{}
	log     chan<- string
}

func (r *SpyRunnable) Name() string {
	return r.name
}

func (r *SpyRunnable) Run() error {
	r.started = true
	<-r.done
	return nil
}

func (r *SpyRunnable) Shutdown() error {
	if r.log != nil {
		r.log <- "stopped " + r.Name()
	}

	r.stopped = true
	r.done <- struct{}{}
	close(r.done)
	return nil
}

func NewBrokenRunnable(err error) *BrokenRunnable {
	return &BrokenRunnable{
		err: err,
	}
}

type BrokenRunnable struct {
	Runnable
	err error
}

func (r *BrokenRunnable) Run() error {
	return r.err
}

func (r *BrokenRunnable) Shutdown() error {
	return r.err
}
