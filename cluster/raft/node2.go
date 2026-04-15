package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
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
	Node2 interface {
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

		Proposer
	}
)

func NewNode2(id uint64, addr string, storage *DiskStorage) Node2 {
	return &node2{
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

		ticker:     time.Tick(100 * time.Millisecond),
		manualTick: make(chan time.Time),

		stop: make(chan struct{}),

		proposalHandlers: make(map[Type]ProposalFunc),
		proposalReporter: NewReporter[result](),

		clients:            cluster.NewClients[Client](),
		confChangeReporter: NewReporter[error](),
	}
}

type node2 struct {
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

	// proposalHandlers is a registry of functions that handle
	// proposals of a given type.
	proposalHandlers map[Type]ProposalFunc
	// proposalReporter returns the result of proposal functions
	// back to the caller.
	proposalReporter Reporter[result]

	clients            cluster.Clients[Client]
	confChangeReporter Reporter[error]
	// TODO: Will need reporters for conf changes and read index requests.
}

// Name implements [process.Runnable.Name].
func (n *node2) Name() string {
	return fmt.Sprintf("raft node %d", n.conf.ID)
}

func (n *node2) AsPeer() cluster.Peer {
	return cluster.Peer{
		ID:   n.conf.ID,
		Addr: n.addr,
	}
}

func (n *node2) Handler() http.Handler {
	h := new(Handler)

	h.node = n

	mux := http.NewServeMux()
	mux.Handle("/message", http.HandlerFunc(h.messageHandler))

	h.Handler = mux

	return h
}

func (n *node2) Receive(msg raftpb.Message) error {
	return n.raft.Step(context.TODO(), msg)
}

// Bootstrap prepares a new Raft node. If this node will join a cluster,
// at least the cluster's leader must be passed as a peer, but it is safer
// to pass all known peers. Bootstrap effectively bypasses the proposal stage
// and adds Peers directly into the Raft node's state.
// It can be called multiple times.
// TODO: Add a test to make sure that duplicate peers are overwritten.
func (n *node2) Bootstrap(peers ...cluster.Peer) {
	for _, peer := range peers {
		// TODO: Return value should be saved in storage.
		n.raft.ApplyConfChange(raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  peer.ID,
			Context: []byte(peer.Addr),
		}.AsV2())

		baseUrl := fmt.Sprintf("http://%s/cluster/raft", peer.Addr)
		client := NewClient(baseUrl)
		peer := cluster.Peer{ID: peer.ID, Addr: peer.Addr}
		n.clients.Add(peer, client)
	}
}

// ConfChangeContext is used as the context data for a Raft ConfChange.
type ConfChangeContext struct {
	// ID is a unique identifier for tracking the ConfChange across the network.
	ID uint64
	// Nodes contains node-specific context index by node ID.
	Nodes map[uint64]string
}

func (c *ConfChangeContext) MustMarshal() []byte {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return data
}

// AddPeer proposes adding the given Peer to the cluster.
// It may be called on any node, not just the leader.
// Proposals may be rejected by the cluster.
func (n *node2) AddPeer(peer cluster.Peer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	id, await := n.confChangeReporter.Await()
	defer n.confChangeReporter.Close(id)

	ccc := &ConfChangeContext{
		ID:    id,
		Nodes: map[uint64]string{peer.ID: peer.Addr},
	}
	err := n.raft.ProposeConfChange(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  peer.ID,
		Context: ccc.MustMarshal(),
	}.AsV2())
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case err := <-await:
		return err
	}
}

func (n *node2) RemovePeer(peer cluster.Peer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	id, await := n.confChangeReporter.Await()
	defer n.confChangeReporter.Close(id)

	ccc := &ConfChangeContext{
		ID:    id,
		Nodes: map[uint64]string{peer.ID: peer.Addr},
	}
	err := n.raft.ProposeConfChange(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeRemoveNode,
		NodeID:  peer.ID,
		Context: ccc.MustMarshal(),
	}.AsV2())
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case err := <-await:
		return err
	}
}

// Name implements [process.Runnable.Run].
func (n *node2) Run() error {
	n.conf.Applied = n.storage.AppliedIndex()
	n.raft = raft.RestartNode(n.conf)
	n.raft.ApplyConfChange(raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: n.conf.ID,
	}.AsV2())

	n.done = make(chan struct{})
	for {
		select {
		case rd := <-n.raft.Ready():
			n.saveToStorage(rd.Entries, rd.HardState, raftpb.Snapshot{}, rd.MustSync)
			n.send(rd.Messages)
			n.process(rd.CommittedEntries)
			n.raft.Advance()
		case <-n.ticker:
			n.raft.Tick()
		case <-n.manualTick:
			n.raft.Tick()
		case <-n.stop:
			n.raft.Stop()
			defer close(n.done)
			return n.storage.Sync()
		}
	}
}

func (n *node2) saveToStorage(entries []raftpb.Entry, hardState raftpb.HardState, _ raftpb.Snapshot, mustSync bool) {
	if err := n.storage.Save(entries, hardState, mustSync); err != nil {
		log.Fatal("failed to persist entries and hardstate:", err)
	}
}

func (n *node2) send(messages []raftpb.Message) {
	for _, message := range messages {
		client, err := n.clients.Get(message.To)
		if err != nil {
			log.Fatalf("no client for node %d", message.To)
		}
		err = client.SendMessage(&message)
		if err != nil {
			log.Printf("failed to send message: %s", err)
		}
	}
}

func (n *node2) process(entries []raftpb.Entry) {
	entries = n.filter(entries)
	// It could be that all entries get filtered out, in which case
	// we can skip having to write AppliedIndex to disk.
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

	if err := n.storage.SaveAppliedIndex(entries[len(entries)-1].Index); err != nil {
		log.Fatal("failed to persist applied index:", err)
	}
}

// filter removes entries for which no actions need to be taken.
func (n *node2) filter(entries []raftpb.Entry) []raftpb.Entry {
	return slices.DeleteFunc(entries, func(e raftpb.Entry) bool {
		// Heartbeats send empty entries, which we don't need to process.
		return len(e.Data) == 0
	})
}

func (n *node2) applyConfChange(cc raftpb.ConfChangeV2) {
	var ccc ConfChangeContext
	err := json.Unmarshal(cc.Context, &ccc)
	if err != nil {
		panic(err)
	}

	for _, change := range cc.Changes {
		switch change.Type {
		case raftpb.ConfChangeAddNode:
			addr, ok := ccc.Nodes[change.NodeID]
			if !ok {
				log.Panicf("node ID not found in conf change context: %d", change.NodeID)
			}
			baseUrl := fmt.Sprintf("http://%s/cluster/raft", addr)
			log.Printf("base url: %s", baseUrl)
			client := NewClient(baseUrl)
			peer := cluster.Peer{ID: change.NodeID, Addr: addr}
			n.clients.Add(peer, client)
		case raftpb.ConfChangeRemoveNode:
			if change.NodeID == n.conf.ID {
				go n.shutdown()
			} else {
				n.clients.Remove(change.NodeID)
			}
		}
	}

	// TODO: Return value should be saved in storage.
	n.raft.ApplyConfChange(cc)

	if ch, ok := n.confChangeReporter.Send(ccc.ID); ok {
		ch <- nil
	}
}

// Name implements [process.Runnable.Shutdown].
func (n *node2) Shutdown() error {
	n.shutdown()
	return nil
}

func (n *node2) shutdown() {
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
