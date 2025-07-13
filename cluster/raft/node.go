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

const (
	storageMaxLogEntries = 1000
)

// TODO: NewNode should return an error instead of panicking? Probably?
func NewNode(id uint64, addr netip.AddrPort, peers []Peer, workDir string, snap SnapshotRestorer) Node {
	storage, err := NewDiskStorage(workDir, nil)
	if err != nil {
		panic(err)
	}

	conf := raft.Config{
		ID:                id,
		ElectionTick:      10,
		HeartbeatTick:     1,
		Storage:           storage,
		MaxSizePerMsg:     64 << 10,
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
	node.snap = snap

	return node
}

type node struct {
	id         uint64
	raft       raft.Node
	ticker     <-chan time.Time
	manualTick chan time.Time
	done       chan struct{}

	Proposer
	mesh    Mesh
	storage *DiskStorage
	snap    SnapshotRestorer
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
			n.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
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
	err := n.snap.Restore(buf)
	if err != nil {
		n.raft.ReportSnapshot(n.id, raft.SnapshotFailure)
		log.Printf("failed to restore snapshot: %s", err)
	}
	n.raft.ReportSnapshot(n.id, raft.SnapshotFinish)
}

func (n *node) processEntries(entries []raftpb.Entry) {
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
			n.raft.ApplyConfChange(cc)
		}
	}

}

func (n *node) Receive(msg *raftpb.Message) error {
	return n.raft.Step(context.TODO(), *msg)
}

func (n *node) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	if err := n.storage.Append(entries); err != nil {
		log.Panicf("failed to append entries to storage: %s\n", err)
	}

	if !raft.IsEmptyHardState(hardState) {
		if err := n.storage.SaveHardState(hardState); err != nil {
			log.Panicf("failed to save hardstate: %s\n", err)
		}
	}

	if !raft.IsEmptySnap(snapshot) {
		if err := n.storage.SaveSnapshot(snapshot); err != nil {
			log.Panicf("failed to apply snapshot: %s\n", err)
		}
	}
}
