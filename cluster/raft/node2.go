package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/robinkb/cascade/cluster"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	Node2 interface {
		Name() string
		Run() error
		Shutdown() error
		Tick()
		Status() raft.Status

		Proposer
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

		proposalHandlers: make(map[Type]ProposalFunc),
		proposalReporter: NewReporter[result](),
	}
}

type node2 struct {
	raft    raft.Node
	storage *raft.MemoryStorage

	ticker     <-chan time.Time
	manualTick chan time.Time

	// stop is signaled when the node needs to shut down the raft state loop.
	stop chan struct{}
	// done waits for the node to finish shutting down the raft state loop.
	done chan struct{}

	// proposalHandlers is a registry of functions that handle
	// proposals of a given type.
	proposalHandlers map[Type]ProposalFunc
	// proposalReporter returns the result of proposal functions
	// back to the caller.
	proposalReporter Reporter[result]
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

				for _, entry := range rd.CommittedEntries {
					// Heartbeats send empty entries, which we don't need to process.
					if len(entry.Data) == 0 {
						continue
					}

					if entry.Type == raftpb.EntryNormal {
						n.commit(entry.Data)
					}
				}

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

func (n *node2) Handle(t Type, f ProposalFunc) {
	if f := n.proposalHandlers[t]; f != nil {
		panic(fmt.Errorf("%w: %d", cluster.ErrDuplicateProposalType, t))
	}
	n.proposalHandlers[t] = f
}

func encodeProposal(id uint64, t uint32, data []byte) []byte {
	header := make([]byte, 12)
	binary.LittleEndian.PutUint64(header, id)
	binary.LittleEndian.PutUint32(header[8:], uint32(t))

	buf := new(bytes.Buffer)
	buf.Write(header)
	buf.Write(data)
	return buf.Bytes()
}

func (n *node2) Propose(t Type, data []byte) (resp any, err error) {
	id, ch := n.proposalReporter.Create()
	defer n.proposalReporter.Delete(id)

	enc := encodeProposal(id, uint32(t), data)

	if err := n.raft.Propose(context.TODO(), enc); err != nil {
		return nil, err
	}

	result := <-ch

	return result.resp, result.err
}

func decodeProposal(enc []byte) (id uint64, t uint32, data []byte) {
	id = binary.LittleEndian.Uint64(enc[0:8])
	t = binary.LittleEndian.Uint32(enc[8:12])
	data = enc[12:]
	return
}

func (n *node2) commit(data []byte) {
	id, pt, dec := decodeProposal(data)

	f, ok := n.proposalHandlers[Type(pt)]
	if !ok {
		panic(fmt.Errorf("%w: %d", cluster.ErrUnknownProposalType, pt))
	}

	resp, err := f(dec)

	ch, ok := n.proposalReporter.Get(id)
	if ok {
		ch <- result{resp, err}
	}
}

type result struct {
	resp any
	err  error
}
