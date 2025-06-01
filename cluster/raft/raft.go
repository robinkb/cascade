package raft

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/robinkb/cascade-registry/cluster"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	nodes = make(map[uint64]*node)
)

func NewRaftNode(id uint64, peers []raft.Peer) cluster.Node {
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

func (n *node) run() {
	for {
		select {
		case <-n.ticker:
			n.raft.Tick()
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
		log.Println("sending message:", raft.DescribeMessage(message, nil))

		peer, ok := nodes[message.To]
		if !ok {
			log.Printf("peer %d not found\n", message.To)
			continue
		}
		peer.receive(n.ctx, message)
	}
}

func (n *node) receive(ctx context.Context, message raftpb.Message) {
	n.raft.Step(ctx, message)
}

func (n *node) process(entry raftpb.Entry) {
	if entry.Data != nil {
		log.Println("normal message:", string(entry.Data))
		// TODO: This is where we would decode the operation in entry.Data
		// and call the wrapped store.Metadata or store.Blobs.
	}
}

func (n *node) processSnapshot(snapshot raftpb.Snapshot) {
	log.Printf("Applying snapshot is not implemented yet")
}
