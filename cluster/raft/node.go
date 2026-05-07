package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"slices"
	"time"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/pkg/process"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	Node interface {
		process.Runnable
		cluster.Proposer
		Tick()
		Status() raft.Status

		AsPeer() cluster.Peer
		Bootstrap(peers ...cluster.Peer)
		AddPeer(peer cluster.Peer) error
		RemovePeer(peer cluster.Peer) error
		Handler() http.Handler
		Receive(m raftpb.Message) error
	}
)

func NewNode(id uint64, addr string, storage *DiskStorage, restorer cluster.Restorer) Node {
	return &node{
		conf: &raft.Config{
			ID:            id,
			HeartbeatTick: 1,
			// HeartbeatTick * 10 is suggested by library
			ElectionTick:    10,
			Storage:         storage,
			MaxSizePerMsg:   4096,
			MaxInflightMsgs: 256,

			StepDownOnRemoval: true,
			PreVote:           true,
		},
		addr:     addr,
		storage:  storage,
		restorer: restorer,

		ticker:     time.Tick(1 * time.Second),
		manualTick: make(chan time.Time),

		stop: make(chan struct{}),

		proposalHandlers: make(map[cluster.ProposalType]cluster.ProposalFunc),
		proposalReporter: NewReporter[result](),

		readStates:        make(map[uint64]uint64),
		readStateReporter: NewReporter[error](),

		clients:            cluster.NewClients[Client](),
		confChangeReporter: NewReporter[error](),
	}
}

type node struct {
	addr     string
	raft     raft.Node
	conf     *raft.Config
	storage  *DiskStorage
	restorer cluster.Restorer

	ticker     <-chan time.Time
	manualTick chan time.Time

	// stop is signaled when the node needs to shut down the raft state loop.
	stop chan struct{}
	// done waits for the node to finish shutting down the raft state loop.
	done chan struct{}

	// proposalHandlers is a registry of functions that handle proposals of a given type.
	proposalHandlers map[cluster.ProposalType]cluster.ProposalFunc
	// proposalReporter returns the result of proposal functions back to the caller.
	proposalReporter Reporter[result]

	readStates        map[uint64]uint64
	readStateReporter Reporter[error]

	clients            cluster.Clients[Client]
	confChangeReporter Reporter[error]
}

// Name implements [process.Runnable.Name].
func (n *node) Name() string {
	return fmt.Sprintf("raft node %d", n.conf.ID)
}

// Name implements [process.Runnable.Run].
func (n *node) Run() error {
	n.conf.Applied = n.storage.AppliedIndex()
	n.raft = raft.RestartNode(n.conf)
	cs := n.raft.ApplyConfChange(raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeId: n.conf.ID,
	}.AsV2())
	n.storage.SetConfState(*cs)

	n.done = make(chan struct{})
	for {
		select {
		case rd := <-n.raft.Ready():
			n.save(rd.Entries, rd.HardState, rd.Snapshot, rd.MustSync)
			n.send(rd.Messages)
			n.restore(rd.Snapshot)
			n.process(rd.CommittedEntries)
			n.check(rd.ReadStates)
			n.raft.Advance()
		case <-n.ticker:
			n.raft.Tick()
		case <-n.manualTick:
			n.raft.Tick()
		case <-n.stop:
			n.raft.Stop()
			close(n.done)
			return n.storage.CreateSnapshot()
		}
	}
}

func (n *node) save(ents []raftpb.Entry, hs raftpb.HardState, sp raftpb.Snapshot, mustSync bool) {
	if !raft.IsEmptyHardState(hs) {
		if err := n.storage.SaveHardState(hs); err != nil {
			log.Fatal("failed to persist hard state:", err)
		}
	}

	if len(ents) > 0 {
		if err := n.storage.SaveEntries(ents); err != nil {
			log.Fatal("failed to persist entries:", err)
		}
	}

	if !raft.IsEmptySnap(sp) {
		if err := n.storage.ApplySnapshot(sp); err != nil {
			log.Fatal("failed to persist snapshot:", err)
		}
	}

	if mustSync {
		if err := n.storage.Sync(); err != nil {
			log.Print("failed to sync disk storage:", err)
		}
	}
}

func (n *node) send(messages []raftpb.Message) {
	for _, message := range messages {
		client, err := n.clients.Get(message.To)
		if err != nil {
			log.Fatalf("%x no client for node %x", n.conf.ID, message.To)
		}
		err = client.SendMessage(&message)
		if err != nil {
			n.raft.ReportUnreachable(message.To)
			log.Printf("%x failed to send message to %x: %s", n.conf.ID, message.To, err)
		}
		if message.Type == raftpb.MessageType_MsgSnap {
			if err == nil {
				n.raft.ReportSnapshot(message.To, raft.SnapshotFinish)
			} else {
				n.raft.ReportSnapshot(message.To, raft.SnapshotFailure)
			}
		}
	}
}

func (n *node) restore(sp raftpb.Snapshot) {
	if raft.IsEmptySnap(sp) {
		return
	}

	leader := n.raft.Status().Lead
	peer, err := n.clients.Peer(leader)
	if err != nil {
		log.Fatalf("%x no client for node %x", n.conf.ID, leader)
	}

	buf := bytes.NewBuffer(sp.Data)
	err = n.restorer.Restore(buf, peer)
	if err != nil {
		log.Fatalf("failed to restore snapshot: %s", err)
	}
}

func (n *node) process(entries []raftpb.Entry) {
	entries = n.filter(entries)
	if len(entries) == 0 {
		return
	}

	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryNormal:
			n.applyProposal(entry.Data)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				panic(err)
			}
			n.applyConfChange(cc.AsV2())
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(entry.Data); err != nil {
				panic(err)
			}
			n.applyConfChange(cc)
		}
	}

	n.storage.SetAppliedIndex(entries[len(entries)-1].Index)
}

// filter removes entries for which no actions need to be taken.
func (n *node) filter(entries []raftpb.Entry) []raftpb.Entry {
	return slices.DeleteFunc(entries, func(e raftpb.Entry) bool {
		// Heartbeats send empty entries, which we don't need to process.
		return len(e.Data) == 0
	})
}

func (n *node) check(rs []raft.ReadState) {
	for _, state := range rs {
		id := binary.LittleEndian.Uint64(state.RequestCtx)
		n.readStates[id] = state.Index
	}

	for id, readIndex := range n.readStates {
		if n.storage.AppliedIndex() >= readIndex {
			if ch, ok := n.readStateReporter.Send(id); ok {
				ch <- nil
			}
		}
	}
}

// Name implements [process.Runnable.Shutdown].
func (n *node) Shutdown() error {
	n.shutdown()
	return nil
}

func (n *node) shutdown() {
	if n.done == nil {
		return
	}

	// The following is a copy from [raft.Node.Stop].
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

func (n *node) Status() raft.Status {
	// We've not started yet.
	if n.done == nil {
		return raft.Status{}
	}
	return n.raft.Status()
}

func (n *node) Tick() {
	n.manualTick <- time.Now()
}
