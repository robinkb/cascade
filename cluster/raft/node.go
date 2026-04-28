package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"slices"
	"time"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/process"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	Node interface {
		process.Runnable
		Name() string
		Run() error
		Shutdown() error
		Tick()
		Status() raft.Status

		AsPeer() cluster.Peer
		Bootstrap(peers ...cluster.Peer)
		AddPeer(peer cluster.Peer) error
		RemovePeer(peer cluster.Peer) error
		Handler() http.Handler
		Receive(m raftpb.Message) error

		cluster.Proposer
	}
)

func NewNode(id uint64, addr string, storage *DiskStorage) Node {
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
		addr:    addr,
		storage: storage,

		ticker:     time.Tick(1 * time.Second),
		manualTick: make(chan time.Time),

		stop: make(chan struct{}),

		proposalHandlers: make(map[cluster.ProposalType]cluster.ProposalFunc),
		proposalReporter: NewReporter[result](),

		clients:            cluster.NewClients[Client](),
		confChangeReporter: NewReporter[error](),
	}
}

type node struct {
	addr    string
	raft    raft.Node
	conf    *raft.Config
	storage *DiskStorage

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

	clients            cluster.Clients[Client]
	confChangeReporter Reporter[error]
}

// Name implements [process.Runnable.Name].
func (n *node) Name() string {
	return fmt.Sprintf("raft node %d", n.conf.ID)
}

func (n *node) AsPeer() cluster.Peer {
	return cluster.Peer{
		ID:   n.conf.ID,
		Addr: n.addr,
	}
}

func (n *node) Handler() http.Handler {
	h := new(Handler)

	h.node = n

	mux := http.NewServeMux()
	mux.Handle("/message", http.HandlerFunc(h.messageHandler))

	h.Handler = mux

	return h
}

func (n *node) Receive(msg raftpb.Message) error {
	return n.raft.Step(context.TODO(), msg)
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
			n.restore(rd.Snapshot)
			n.save(rd.Entries, rd.HardState, rd.MustSync)
			n.send(rd.Messages)
			n.process(rd.CommittedEntries)
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

func (n *node) restore(sp raftpb.Snapshot) {
	if raft.IsEmptySnap(sp) {
		return
	}

	if err := n.storage.ApplySnapshot(sp); err != nil {
		log.Fatal("failed to persist snapshot:", err)
	}
}

func (n *node) save(entries []raftpb.Entry, hardState raftpb.HardState, mustSync bool) {
	if err := n.storage.Save(entries, hardState, mustSync); err != nil {
		log.Fatal("failed to persist entries and hardstate:", err)
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
			log.Printf("%x failed to send message to %x: %s", n.conf.ID, message.To, err)
		}
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

func (n *node) Handle(t cluster.ProposalType, f cluster.ProposalFunc) {
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

func (n *node) Propose(t cluster.ProposalType, data []byte) (resp any, err error) {
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

func (n *node) applyProposal(data []byte) {
	id, pt, dec := decodeProposal(data)

	f, ok := n.proposalHandlers[cluster.ProposalType(pt)]
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
