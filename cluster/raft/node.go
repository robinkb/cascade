package raft

import (
	"bytes"
	"context"
	"log"
	"net/netip"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	Node interface { // Represents everything that a Raft node has to do for Raft to work
		// Lifecycle
		Start()
		Stop()
		Tick()
		ClusterStatus() Status

		// Messaging
		Receive(m *raftpb.Message) error

		Proposer
	}

	Status struct {
		Clustered bool
	}
)

// TODO: NewNode should return an error instead of panicking? Probably?
// Also, I should probably decompose this more and allow passing dependencies
// like a Mesh and DiskStorage directly.
func NewNode(id uint64, addr netip.AddrPort, peers []Peer, workDir string, snap SnapshotRestorer) Node {
	storage, err := NewDiskStorage(workDir, snap, nil)
	if err != nil {
		panic(err)
	}

	conf := raft.Config{
		ID:                id,
		ElectionTick:      10,
		HeartbeatTick:     1,
		Storage:           storage,
		MaxSizePerMsg:     1 << 20,
		MaxInflightMsgs:   256,
		StepDownOnRemoval: true,
	}

	raftPeers := make([]raft.Peer, len(peers))
	for i := range peers {
		raftPeers[i] = raft.Peer{ID: peers[i].ID}
	}

	clients := make(map[uint64]*Client, len(peers))
	for _, peer := range peers {
		clients[peer.ID] = NewClient("http://" + peer.AddrPort.String())
	}

	node := &node{
		id:         id,
		raft:       raft.StartNode(&conf, raftPeers),
		storage:    storage,
		ticker:     time.Tick(1 * time.Second),
		manualTick: make(chan time.Time),
		done:       make(chan struct{}),
	}

	node.Proposer = NewProposer(node.raft)
	node.mesh = NewMesh(node, addr)
	for _, peer := range peers {
		node.mesh.SetPeer(peer.ID, peer.AddrPort)
	}
	node.restorer = snap

	return node
}

type node struct {
	id         uint64
	raft       raft.Node
	ticker     <-chan time.Time
	manualTick chan time.Time
	done       chan struct{}

	Proposer
	mesh     Mesh
	storage  *DiskStorage
	restorer Restorer
}

func (n *node) Start() {
	go n.run()
	go n.mesh.Start()
}

func (n *node) Stop() {}

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

		if message.Type == raftpb.MsgSnap {
			// TODO: Snapshotting may fail, and that has to be reported through this method.
			n.raft.ReportSnapshot(message.To, raft.SnapshotFinish)
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
				n.Commit(entry.Data)
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				log.Panicf("could not read ConfChange entry: %s", err)
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
