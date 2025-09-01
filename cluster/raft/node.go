package raft

import (
	"bytes"
	"context"
	"log"
	"net/netip"
	"time"

	"github.com/robinkb/cascade-registry/cluster"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	// TODO: This is a dumb interface.
	Node interface {
		Start()
		Stop() error
		Tick()
		ClusterStatus() Status

		// Messaging
		Receive(m *raftpb.Message) error

		cluster.Proposer
	}

	Status struct {
		Clustered bool
	}
)

// TODO: NewNode should return an error instead of panicking? Probably?
// Also, I should probably decompose this more and allow passing dependencies
// like a Mesh directly.
func NewNode(id uint64, addr netip.AddrPort, peers []Peer, storage *DiskStorage, snap cluster.SnapshotRestorer) Node {
	conf := raft.Config{
		// TODO: This may need to be set when restarting a node.
		// But I'm not sure of how to persist it. It can only be saved _after_
		// applying entries to the state machine. And etcd doesn't seem to set this either.
		Applied: 0,

		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,

		// If weird stuff happens, I should check these toggles.
		StepDownOnRemoval: true,
		PreVote:           true,
		CheckQuorum:       true,
	}

	node := &node{
		id:         id,
		addr:       addr,
		raft:       raft.RestartNode(&conf),
		storage:    storage,
		ticker:     time.Tick(100 * time.Millisecond),
		manualTick: make(chan time.Time),
		done:       make(chan struct{}),
		confChanges: errMap{
			errs: make(map[uint64]chan error),
		},
	}

	node.proposer = newProposer(node.raft)
	node.mesh = NewMesh(node, addr)
	node.restorer = snap

	return node
}

type node struct {
	id         uint64
	addr       netip.AddrPort
	raft       raft.Node
	ticker     <-chan time.Time
	manualTick chan time.Time
	done       chan struct{}

	confChanges errMap

	*proposer
	mesh     Mesh
	storage  *DiskStorage
	restorer cluster.Restorer
}

// Bootstrap prepares a new Raft node. If this node will join a cluster,
// at least the cluster's leader must be passed as a peer, but it is safer
// to pass all known peers.
func (n *node) Bootstrap(peers ...Peer) {
	peers = append(peers, Peer{
		ID:       n.raft.Status().ID,
		AddrPort: n.addr,
	})

	for _, peer := range peers {
		confState := n.raft.ApplyConfChange(raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  peer.ID,
			Context: []byte(peer.AddrPort.String()),
		}.AsV2())

		n.storage.SaveConfState(*confState)

		if n.id != peer.ID {
			n.mesh.SetPeer(peer.ID, peer.AddrPort)
		}
	}
}

// AddNode proposes adding the given Peer to the cluster.
// Proposals may be rejected by the cluster. AddNode blocks
// until the Peer is added, an error is encountered,
// or until the context is cancelled.
func (n *node) AddNode(ctx context.Context, peer Peer) error {
	return n.proposeConfChange(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  peer.ID,
		Context: []byte(peer.AddrPort.String()),
	})
}

func (n *node) proposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	// TODO: Not really safe to use node ID as a key to track individual conf changes.
	// Multiple conf changes about the same node may be underway at once.
	// A unique ID should be generated per conf change instead.
	errC := n.confChanges.create(cc.NodeID)
	defer n.confChanges.delete(cc.NodeID)

	err := n.raft.ProposeConfChange(ctx, cc.AsV2())
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case err := <-errC:
		return err
	}
}

func (n *node) Start() {
	go n.run()
	go n.mesh.Start()
}

func (n *node) Stop() error {
	return n.storage.deck.Close()
}

func (n *node) Tick() {
	n.manualTick <- time.Now()
}

func (n *node) ClusterStatus() Status {
	status := Status{}

	if n.raft.Status().Lead != 0 {
		status.Clustered = true
	}

	return status
}

func (n *node) run() {
	for {
		select {
		case rd := <-n.raft.Ready():
			n.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot, rd.MustSync)
			n.send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				n.processSnapshot(rd.Snapshot)
			}
			n.processEntries(rd.CommittedEntries)
			n.raft.Advance()
		case <-n.ticker:
			n.raft.Tick()
		case <-n.manualTick:
			n.raft.Tick()
		case <-n.done:
			return
		}
	}
}

func (n *node) send(messages []raftpb.Message) {
	for _, message := range messages {
		err := n.mesh.SendMessage(message.To, &message)
		if err != nil {
			log.Println("failed to send message:", err)
		}
	}
}

func (n *node) processSnapshot(snap raftpb.Snapshot) {
	buf := bytes.NewBuffer(snap.Data)
	err := n.restorer.Restore(buf)
	if err != nil {
		log.Printf("failed to restore snapshot: %s", err)
		n.raft.ReportSnapshot(n.id, raft.SnapshotFailure)
	}
	n.raft.ReportSnapshot(n.id, raft.SnapshotFinish)
}

func (n *node) processEntries(entries []raftpb.Entry) {
	if len(entries) == 0 {
		return
	}

	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryNormal:
			if entry.Data != nil {
				n.commit(entry.Data)
			}
		case raftpb.EntryConfChange:
			panic("received EntryConfChange v1; must be v2")
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(entry.Data); err != nil {
				log.Panicf("could not read ConfChange entry: %s", err)
			}

			for _, change := range cc.Changes {
				switch change.Type {
				case raftpb.ConfChangeAddNode:
					url := netip.MustParseAddrPort(string(cc.Context))
					n.mesh.SetPeer(change.NodeID, url)
					log.Printf("%d added node with id %d and url %s", n.raft.Status().ID, change.NodeID, url.String())
				case raftpb.ConfChangeRemoveNode:
					log.Printf("%d removed node with id %d", n.raft.Status().ID, change.NodeID)
					n.mesh.DeletePeer(change.NodeID)
				}
				if errC, ok := n.confChanges.get(change.NodeID); ok {
					errC <- nil
				}
			}
			cs := n.raft.ApplyConfChange(cc)
			n.storage.SaveConfState(*cs)
		}
	}

	// TODO: Commit should return an error or something to signal
	// if the commit was successfully applied. We can't just set
	// AppliedIndex to the last Entry's Index.
	n.storage.AppliedIndex(entries[len(entries)-1].Index)
}

func (n *node) Receive(msg *raftpb.Message) error {
	return n.raft.Step(context.TODO(), *msg)
}

func (n *node) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot, mustSync bool) {
	if err := n.storage.Save(entries, hardState, mustSync); err != nil {
		log.Panicf("failed to append entries and hardstate to storage: %s", err)
	}

	if !raft.IsEmptySnap(snapshot) {
		if err := n.storage.SaveSnapshot(snapshot); err != nil {
			log.Panicf("failed to apply snapshot: %s\n", err)
		}
	}
}
