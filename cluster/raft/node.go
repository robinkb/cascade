package raft

import (
	"context"
	"errors"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
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
func NewNode(id uint64, addr netip.AddrPort, peers []Peer, workDir string) Node {
	logFile := filepath.Join(workDir, "raft.log")
	w, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	r, err := os.OpenFile(logFile, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}

	storage, err := storage.NewLog(r, w)
	if err != nil {
		panic(err)
	}

	conf := raft.Config{
		ID:                 id,
		ElectionTick:       10,
		HeartbeatTick:      1,
		Storage:            storage,
		MaxSizePerMsg:      64 << 10,
		MaxInflightMsgs:    256,
		StepDownOnRemoval:  true,
		AsyncStorageWrites: true,
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

		append: make(chan raftpb.Message),
		apply:  make(chan raftpb.Message),
	}

	node.Proposer = NewProposer(node.raft)
	node.mesh = NewMesh(node, addr)
	for _, peer := range peers {
		node.mesh.SetPeer(peer.ID, peer.AddrPort)
	}

	return node
}

type node struct {
	id         uint64
	raft       raft.Node
	ticker     <-chan time.Time
	manualTick chan time.Time
	done       chan struct{}

	append chan raftpb.Message
	apply  chan raftpb.Message

	Proposer
	mesh    Mesh
	storage *storage.Log
}

func (n *node) Start() {
	go n.appender()
	go n.applier()
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
		case <-n.ticker:
			n.raft.Tick()
		case rd := <-n.raft.Ready():
			for _, m := range rd.Messages {
				switch m.To {
				case raft.LocalAppendThread:
					n.append <- m
				case raft.LocalApplyThread:
					n.apply <- m
				default:
					n.send([]raftpb.Message{m})
				}
			}
		case <-n.manualTick:
			n.raft.Tick()
		case <-n.done:
			return
		}
	}
}

func (n *node) appender() {
	for {
		select {
		case m := <-n.append:
			n.saveToStorage(raftpb.HardState{
				Term:   m.Term,
				Vote:   m.Vote,
				Commit: m.Commit,
			}, m.Entries, m.Snapshot)
			n.send(m.Responses)
		case <-n.done:
			return
		}
	}
}

func (n *node) applier() {
	for {
		select {
		case m := <-n.apply:
			for _, entry := range m.Entries {
				switch entry.Type {
				case raftpb.EntryNormal:
					n.process(entry)
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					n.raft.ApplyConfChange(cc)
				}
			}
			n.send(m.Responses)
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

func (n *node) process(entry raftpb.Entry) {
	if entry.Data != nil {
		n.Commit(entry.Data)
	}
}

func (n *node) Receive(msg *raftpb.Message) error {
	return n.raft.Step(context.TODO(), *msg)
}

func (n *node) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot *raftpb.Snapshot) {
	if err := n.storage.Append(entries); err != nil {
		log.Panicf("failed to append entries to storage: %s\n", err)
	}

	if !raft.IsEmptyHardState(hardState) {
		if err := n.storage.SetHardState(hardState); err != nil {
			log.Panicf("failed to save hardstate: %s\n", err)
		}
	}

	if snapshot != nil && !raft.IsEmptySnap(*snapshot) {
		if err := n.storage.ApplySnapshot(*snapshot); err != nil {
			log.Panicf("failed to apply snapshot: %s\n", err)
		}
	}
}

func (n *node) compact() {
	// This can't actually fail with in-memory raft storage.
	li, _ := n.storage.LastIndex()
	if li > storageMaxLogEntries {
		err := n.storage.Compact(li - storageMaxLogEntries)
		if err != nil && !errors.Is(err, raft.ErrCompacted) {
			log.Panicln("unexpected error while compacting raft log:", err)
		}
	}
}
