package raft

import (
	"time"

	"go.etcd.io/raft/v3"
)

type (
	Node2 interface {
		Name() string
		Run() error
		Shutdown() error
		Tick()

		Status() raft.Status
	}
)

func NewNode2(dir string) Node2 {
	storage := raft.NewMemoryStorage()
	return &node2{
		raft: raft.StartNode(&raft.Config{
			ID:              1,
			HeartbeatTick:   1,
			ElectionTick:    10, // HeartbeatTick * 10 is suggested by library
			Storage:         storage,
			MaxSizePerMsg:   4096,
			MaxInflightMsgs: 256,
		}, []raft.Peer{{ID: 1}}),

		storage:    storage,
		ticker:     time.Tick(100 * time.Millisecond),
		manualTick: make(chan time.Time),

		stop: make(chan struct{}),
	}
}

type node2 struct {
	raft    raft.Node
	storage *raft.MemoryStorage

	ticker     <-chan time.Time
	manualTick chan time.Time

	// stop is signalled when the node needs to shut down the raft state loop.
	stop chan struct{}
	// done waits for the node to finish shutting down the raft state loop.
	done chan struct{}
}

func (n *node2) Name() string {
	return ""
}

func (n *node2) Run() error {
	n.done = make(chan struct{})

	go func() {
		for {
			select {
			case rd := <-n.raft.Ready():
				n.storage.SetHardState(rd.HardState)
				n.storage.Append(rd.Entries)

				n.raft.Advance()
			case <-n.ticker:
				n.raft.Tick()
			case <-n.manualTick:
				n.raft.Tick()
			case <-n.stop:
				close(n.done)
				return
			}
		}
	}()
	return nil
}

func (n *node2) Shutdown() error {
	n.raft.Stop()
	// The following is a copy from [raft.Node.Stop].
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return nil
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
	return nil
}

func (n *node2) Status() raft.Status {
	return n.raft.Status()
}

func (n *node2) Tick() {
	n.manualTick <- time.Now()
}
