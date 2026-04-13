package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"slices"
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

func NewNode2(storage *DiskStorage) Node2 {
	return &node2{
		conf: &raft.Config{
			ID:            1,
			HeartbeatTick: 1,
			// HeartbeatTick * 10 is suggested by library
			ElectionTick:    10,
			Storage:         storage,
			MaxSizePerMsg:   4096,
			MaxInflightMsgs: 256,
		},
		storage: storage,

		ticker:     time.Tick(100 * time.Millisecond),
		manualTick: make(chan time.Time),

		stop: make(chan struct{}),

		proposalHandlers: make(map[Type]ProposalFunc),
		proposalReporter: NewReporter[result](),
	}
}

type node2 struct {
	raft    raft.Node
	conf    *raft.Config
	storage *DiskStorage

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
	// TODO: Will need reporters for conf changes and read index requests.
}

func (n *node2) Name() string {
	return fmt.Sprintf("raft node %d", n.conf.ID)
}

func (n *node2) Run() error {
	n.conf.Applied = n.storage.AppliedIndex()
	n.raft = raft.RestartNode(n.conf)
	n.raft.ApplyConfChange(raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: n.conf.ID,
	}.AsV2())

	n.done = make(chan struct{})
	go func() {
		for {
			select {
			case rd := <-n.raft.Ready():
				n.saveToStorage(rd.Entries, rd.HardState, raftpb.Snapshot{}, rd.MustSync)
				n.processCommittedEntries(rd.CommittedEntries)
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

func (n *node2) saveToStorage(entries []raftpb.Entry, hardState raftpb.HardState, _ raftpb.Snapshot, mustSync bool) {
	if err := n.storage.Save(entries, hardState, mustSync); err != nil {
		log.Fatal("failed to persist entries and hardstate:", err)
	}
}

func (n *node2) processCommittedEntries(entries []raftpb.Entry) {
	entries = n.filterEmptyEntries(entries)
	// It could be that all entries get filtered out, in which case
	// we can skip having to write AppliedIndex to disk.
	if len(entries) == 0 {
		return
	}

	for _, entry := range entries {
		if entry.Type == raftpb.EntryNormal {
			n.applyProposal(entry.Data)
		}
	}

	if err := n.storage.SaveAppliedIndex(entries[len(entries)-1].Index); err != nil {
		log.Fatal("failed to persist applied index:", err)
	}
}

// filterEmptyEntries removes entries for which no actions need to be taken.
func (n *node2) filterEmptyEntries(entries []raftpb.Entry) []raftpb.Entry {
	return slices.DeleteFunc(entries, func(e raftpb.Entry) bool {
		// Heartbeats send empty entries, which we don't need to process.
		return len(e.Data) == 0
	})
}

func (n *node2) Shutdown() error {
	if n.done == nil {
		return nil
	}

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

	return n.storage.Sync()
}

func (n *node2) Status() raft.Status {
	// We've not started yet.
	if n.done == nil {
		return raft.Status{}
	}
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
	id, await := n.proposalReporter.Await()
	defer n.proposalReporter.Close(id)

	enc := encodeProposal(id, uint32(t), data)
	if err := n.raft.Propose(context.TODO(), enc); err != nil {
		return nil, err
	}

	result := <-await
	return result.resp, result.err
}

func decodeProposal(enc []byte) (id uint64, t uint32, data []byte) {
	id = binary.LittleEndian.Uint64(enc[0:8])
	t = binary.LittleEndian.Uint32(enc[8:12])
	data = enc[12:]
	return
}

func (n *node2) applyProposal(data []byte) {
	id, pt, dec := decodeProposal(data)

	f, ok := n.proposalHandlers[Type(pt)]
	if !ok {
		panic(fmt.Errorf("%w: %d", cluster.ErrUnknownProposalType, pt))
	}

	resp, err := f(dec)

	send, ok := n.proposalReporter.Send(id)
	if ok {
		send <- result{resp, err}
	} else if err != nil {
		log.Println("application error applying proposal:", err)
	}
}

type result struct {
	resp any
	err  error
}
