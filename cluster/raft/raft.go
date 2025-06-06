package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/cluster"
	"github.com/robinkb/cascade-registry/store"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	nodes = make(map[uint64]*node)
)

type Peer struct {
	ID   uint64
	Addr net.TCPAddr
}

func NewRaftNode(id uint64, addr *net.TCPAddr, peers []Peer, metadata store.Metadata) cluster.Node {
	storage := raft.NewMemoryStorage()
	conf := raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint16,
		MaxInflightMsgs: 256,
	}

	npeers := make(map[uint64]Peer)
	raftPeers := make([]raft.Peer, len(peers))
	for i := range peers {
		raftPeers[i] = raft.Peer{ID: peers[i].ID}
		npeers[peers[i].ID] = peers[i]
	}

	node := &node{
		raft:    raft.StartNode(&conf, raftPeers),
		storage: storage,
		ticker:  time.Tick(10 * time.Millisecond),
		errors:  make(map[uint64]chan error),

		addr:  addr,
		peers: npeers,

		Metadata: metadata,
	}

	nodes[id] = node

	return node
}

type node struct {
	raft    raft.Node
	ticker  <-chan time.Time
	storage *raft.MemoryStorage
	done    <-chan struct{}
	errors  map[uint64]chan error

	peers map[uint64]Peer
	addr  *net.TCPAddr

	store.Metadata
}

func (n *node) Start() {
	go n.run()
	go n.receive()
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

func (n *node) send(messages []raftpb.Message) {
	for _, message := range messages {

		peer, ok := n.peers[message.To]
		if !ok {
			log.Printf("peer %d not found\n", message.To)
			continue
		}

		conn, err := net.DialTCP("tcp", nil, &peer.Addr)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()

		data, err := proto.Marshal(&message)
		if err != nil {
			log.Fatal(err)
		}

		io.Copy(conn, bytes.NewBuffer(data))

		if message.Type == raftpb.MsgSnap {
			// TODO: Snapshotting may fail, and that has to be reported through this method.
			n.raft.ReportSnapshot(message.To, raft.SnapshotFinish)
		}
	}
}

func (n *node) receive() {
	l, err := net.ListenTCP("tcp", n.addr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			defer c.Close()

			buf := &bytes.Buffer{}
			io.Copy(buf, c)

			var message raftpb.Message
			if err := message.Unmarshal(buf.Bytes()); err == nil {
				n.raft.Step(context.TODO(), message)
			}
		}(conn)
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

func (n *node) CreateRepository(name string) error {
	op := &createRepository{
		rand.Uint64(),
		name,
	}
	return n.propose(op)
}

func (n *node) DeleteRepository(name string) error {
	op := &deleteRepository{
		rand.Uint64(),
		name,
	}
	return n.propose(op)
}

func (n *node) PutBlob(name string, digest digest.Digest) error {
	op := &putBlob{
		rand.Uint64(),
		name, digest,
	}
	return n.propose(op)
}

func (n *node) DeleteBlob(name string, digest digest.Digest) error {
	op := &deleteBlob{
		rand.Uint64(),
		name, digest,
	}
	return n.propose(op)
}

func (n *node) PutManifest(name string, digest digest.Digest, meta *store.ManifestMetadata) error {
	op := &putManifest{
		rand.Uint64(),
		name, digest, meta,
	}
	return n.propose(op)
}

func (n *node) DeleteManifest(name string, digest digest.Digest) error {
	op := &deleteManifest{
		rand.Uint64(),
		name, digest,
	}
	return n.propose(op)
}

func (n *node) PutTag(name, tag string, digest digest.Digest) error {
	op := &putTag{
		rand.Uint64(),
		name, tag, digest,
	}

	return n.propose(op)
}

func (n *node) DeleteTag(name, tag string) error {
	op := &deleteTag{
		rand.Uint64(),
		name, tag,
	}

	return n.propose(op)
}

func (n *node) propose(o operation) error {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(&o)

	n.errors[o.ID()] = make(chan error)
	// Propose blocks until accepted by the cluster.
	// TODO: Find out how to retry proposals.
	err := n.raft.Propose(context.TODO(), buf.Bytes())
	if err != nil {
		return err
	}

	select {
	case <-time.Tick(5 * time.Second):
		panic("timed out")
	case <-n.errors[o.ID()]:
		return err
	}
}

func (n *node) process(entry raftpb.Entry) {
	if entry.Data != nil {
		buf := bytes.NewBuffer(entry.Data)

		var c operation
		err := gob.NewDecoder(buf).Decode(&c)
		if err != nil {
			log.Fatalf("unable to decode as operation: %s", err)
		}

		switch v := c.(type) {
		case *createRepository:
			err = n.Metadata.CreateRepository(v.Name)
		case *deleteRepository:
			err = n.Metadata.DeleteRepository(v.Name)
		case *putBlob:
			err = n.Metadata.PutBlob(v.Name, v.Digest)
		case *deleteBlob:
			err = n.Metadata.DeleteBlob(v.Name, v.Digest)
		case *putManifest:
			err = n.Metadata.PutManifest(v.Name, v.Digest, v.Meta)
		case *deleteManifest:
			err = n.Metadata.DeleteManifest(v.Name, v.Digest)
		case *putTag:
			err = n.Metadata.PutTag(v.Name, v.Tag, v.Digest)
		case *deleteTag:
			err = n.Metadata.DeleteTag(v.Name, v.Tag)
		default:
			log.Fatalf("unknown operation received: %T", v)
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
