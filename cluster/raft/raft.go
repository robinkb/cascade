package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"math"
	"math/rand/v2"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/cluster"
	"github.com/robinkb/cascade-registry/store"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	nodes = make(map[uint64]*node)
)

func NewRaftNode(id uint64, peers []raft.Peer, metadata store.Metadata) cluster.Node {
	storage := raft.NewMemoryStorage()
	conf := raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint16,
		MaxInflightMsgs: 256,
	}

	node := &node{
		ctx:     context.Background(),
		raft:    raft.StartNode(&conf, peers),
		storage: storage,
		ticker:  time.Tick(10 * time.Millisecond),
		errors:  make(map[uint64]chan error),

		Metadata: metadata,
	}

	nodes[id] = node

	return node
}

type node struct {
	ctx     context.Context
	raft    raft.Node
	ticker  <-chan time.Time
	storage *raft.MemoryStorage
	done    <-chan struct{}
	errors  map[uint64]chan error

	store.Metadata
}

func (n *node) Start() {
	go n.run()
}

func (n *node) ClusterStatus() cluster.Status {
	status := cluster.Status{}

	if n.raft.Status().Lead != 0 {
		status.Clustered = true
	}

	return status
}

func (n *node) PutTag(name, tag string, digest digest.Digest) error {
	putTag := &putTag{
		rand.Uint64(),
		name, tag, digest,
	}

	return n.propose(putTag)
}

func (n *node) propose(m operation) error {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(&m)

	n.errors[m.ID()] = make(chan error)
	// Propose blocks until accepted by the cluster.
	// TODO: Find out how to retry proposals.
	err := n.raft.Propose(context.TODO(), buf.Bytes())
	if err != nil {
		return err
	}

	select {
	case <-time.Tick(5 * time.Second):
		panic("timed out")
	case <-n.errors[m.ID()]:
		return err
	}
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
			for _, entry := range rd.CommittedEntries {
				switch entry.Type {
				case raftpb.EntryNormal:
					n.process(entry)
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					n.raft.ApplyConfChange(cc)
				}
			}
			n.raft.Advance()
		case <-n.ticker:
			n.raft.Tick()
		case <-n.done:
			return
		}
	}
}

func (n *node) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	n.storage.Append(entries)

	if !raft.IsEmptyHardState(hardState) {
		n.storage.SetHardState(hardState)
	}

	if !raft.IsEmptySnap(snapshot) {
		n.storage.ApplySnapshot(snapshot)
	}
}

func (n *node) send(messages []raftpb.Message) {
	for _, message := range messages {
		peer, ok := nodes[message.To]
		if !ok {
			log.Printf("peer %d not found\n", message.To)
			continue
		}
		peer.receive(n.ctx, message)

		if message.Type == raftpb.MsgSnap {
			// TODO: Snapshotting may fail, and that has to be reported through this method.
			n.raft.ReportSnapshot(message.To, raft.SnapshotFinish)
		}
	}
}

func (n *node) receive(ctx context.Context, message raftpb.Message) {
	n.raft.Step(ctx, message)
}

func (n *node) process(entry raftpb.Entry) {
	if entry.Data != nil {
		buf := bytes.NewBuffer(entry.Data)

		var c operation
		err := gob.NewDecoder(buf).Decode(&c)
		if err != nil {
			log.Fatal(err)
		}

		switch v := c.(type) {
		case *putTag:
			err = n.Metadata.PutTag(v.Name, v.Tag, v.Digest)
		default:
			log.Fatalf("unexpected type received: %v", v)
		}

		errC, ok := n.errors[c.ID()]
		if ok {
			if err != nil {
				errC <- err
			} else {
				close(errC)
				delete(n.errors, c.ID())
			}
		}
	}
}

func (n *node) processSnapshot(snapshot raftpb.Snapshot) {
	log.Printf("Applying snapshot is not implemented yet")
}
