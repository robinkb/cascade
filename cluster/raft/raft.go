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
					if err := cc.Unmarshal(entry.Data); err != nil {
						log.Panicf("could not read ConfChange entry: %s", err)
					}
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
		defer func() {
			if err := conn.Close(); err != nil {
				log.Printf("failed to close connection: %s\n", err)
			}
		}()

		data, err := proto.Marshal(&message)
		if err != nil {
			log.Fatal(err)
		}

		if _, err := io.Copy(conn, bytes.NewBuffer(data)); err != nil {
			log.Panicf("failed to copy message data into buffer: %s\n", err)
		}

		if message.Type == raftpb.MsgSnap {
			// TODO: Snapshotting may fail, and that has to be reported through this method.
			n.raft.ReportSnapshot(message.To, raft.SnapshotFinish)
		}
	}
}

func (n *node) receive() {
	l, err := net.ListenTCP("tcp", n.addr)
	if err != nil {
		log.Panicf("failed to open receiving TCP connection: %s\n", err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			log.Printf("failed to close receiving connection: %s\n", err)
		}
	}()

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Printf("failed to accept receive connection: %s\n", err)
			return
		}

		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			defer func() {
				if err := c.Close(); err != nil {
					log.Printf("failed to close connection: %s\n", err)
				}
			}()

			buf := &bytes.Buffer{}
			if _, err := io.Copy(buf, c); err != nil {
				log.Panicf("failed to copy message data into buffer: %s\n", err)
			}

			var message raftpb.Message
			if err := message.Unmarshal(buf.Bytes()); err != nil {
				log.Panicf("failed to decode raft message: %s\n", err)
			}
			if err := n.raft.Step(context.TODO(), message); err != nil {
				log.Panicf("failed to advance raft state machine: %s\n", err)
			}
		}(conn)
	}
}

func (n *node) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	if err := n.storage.Append(entries); err != nil {
		log.Panicf("failed to append entries to storage: %s\n", err)
	}

	if !raft.IsEmptyHardState(hardState) {
		if err := n.storage.SetHardState(hardState); err != nil {
			log.Panicf("failed to save hardstate: %s\n", err)
		}
	}

	if !raft.IsEmptySnap(snapshot) {
		if err := n.storage.ApplySnapshot(snapshot); err != nil {
			log.Panicf("failed to apply snapshot: %s\n", err)
		}
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

func (n *node) PutUploadSession(name string, session *store.UploadSession) error {
	op := &putUploadSession{
		rand.Uint64(),
		name, session,
	}
	return n.propose(op)
}

func (n *node) DeleteUploadSession(name string, id string) error {
	op := &deleteUploadSession{
		rand.Uint64(),
		name, id,
	}
	return n.propose(op)
}

func (n *node) propose(o operation) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&o); err != nil {
		log.Panicf("failed to encode operation: %s\n", err)
	}

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
			log.Panicf("unable to decode as operation: %s", err)
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
		case *putUploadSession:
			err = n.Metadata.PutUploadSession(v.Name, v.Session)
		case *deleteUploadSession:
			err = n.Metadata.DeleteUploadSession(v.Name, v.SessionID)
		default:
			log.Panicf("unknown operation received: %T", v)
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
