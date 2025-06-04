package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
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
	putTag := putTag{name, tag, digest}
	var argBuf bytes.Buffer
	gob.NewEncoder(&argBuf).Encode(&putTag)

	var opBuf bytes.Buffer
	operation := &operation{
		ID:     rand.Uint64(),
		Method: "PutTag",
		Args:   argBuf.Bytes(),
	}
	gob.NewEncoder(&opBuf).Encode(&operation)

	n.errors[operation.ID] = make(chan error)
	// Propose blocks until accepted by the cluster.
	// TODO: Find out how to retry proposals.
	err := n.raft.Propose(context.TODO(), opBuf.Bytes())
	if err != nil {
		return err
	}

	select {
	case <-time.Tick(1 * time.Second):
		panic("timed out")
	case <-n.errors[operation.ID]:
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
		log.Println("sending message:", raft.DescribeMessage(message, nil))

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
		log.Println("normal message:", string(entry.Data))
		n.decode(bytes.NewBuffer(entry.Data))
	}
}

func (n *node) processSnapshot(snapshot raftpb.Snapshot) {
	log.Printf("Applying snapshot is not implemented yet")
}

type (
	operation struct {
		ID     uint64
		Method string
		Args   []byte
	}

	putTag struct {
		Name, Tag string
		Digest    digest.Digest
	}
)

func (n *node) decode(r io.Reader) {
	var operation operation
	err := gob.NewDecoder(r).Decode(&operation)
	if err != nil {
		panic(err)
	}

	switch operation.Method {
	case "PutTag":
		var putTag putTag
		err := gob.NewDecoder(bytes.NewBuffer(operation.Args)).Decode(&putTag)
		if err != nil {
			panic(err)
		}
		err = n.Metadata.PutTag(putTag.Name, putTag.Tag, putTag.Digest)
		if n.errors[operation.ID] != nil {
			if err != nil {
				n.errors[operation.ID] <- err
			} else {
				close(n.errors[operation.ID])
				delete(n.errors, operation.ID)
			}
		}
	}
}
