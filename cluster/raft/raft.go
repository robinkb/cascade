package raft

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"math"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/robinkb/cascade-registry/cluster"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Peer struct {
	ID   uint64
	Addr *net.TCPAddr
}

func NewNode(id uint64, addr *net.TCPAddr, peers []Peer) cluster.Node {
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
		raft:       raft.StartNode(&conf, raftPeers),
		storage:    storage,
		ticker:     time.Tick(1 * time.Second),
		manualTick: make(chan time.Time),
		errors: errors{
			errs: make(map[uint64]chan error),
		},

		addr:     addr,
		peers:    npeers,
		messages: make(map[uint64]chan raftpb.Message),

		handlerFuncs: make(map[reflect.Type]cluster.HandlerFunc),
	}

	return node
}

type node struct {
	raft       raft.Node
	ticker     <-chan time.Time
	manualTick chan time.Time
	storage    *raft.MemoryStorage
	done       <-chan struct{}
	errors     errors

	peers    map[uint64]Peer
	addr     *net.TCPAddr
	messages map[uint64]chan raftpb.Message

	handlerFuncs map[reflect.Type]cluster.HandlerFunc
}

func (n *node) Start() {
	go n.run()
	go n.receive()
	for _, peer := range n.peers {
		if peer.ID != n.raft.Status().ID {
			go n.send(peer)
			n.messages[peer.ID] = make(chan raftpb.Message)
		}
	}
}

func (n *node) Tick() {
	n.manualTick <- time.Now()
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
			for _, message := range rd.Messages {
				n.messages[message.To] <- message
			}
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
		case <-n.manualTick:
			n.raft.Tick()
		case <-n.done:
			return
		}
	}
}

func (n *node) send(peer Peer) {
	for {
		conn, err := net.DialTCP("tcp", nil, peer.Addr)
		if err != nil {
			log.Println("failed to connect to peer:", err)
			time.Sleep(1 * time.Millisecond)
			continue
		}
		defer conn.Close()

		varint := make([]byte, 4)
		for message := range n.messages[peer.ID] {
			if message.To != peer.ID {
				log.Panicln("received message for the wrong peer")
			}

			log.Println(n.raft.Status().ID, "sending message to", message.To)
			data, err := proto.Marshal(&message)
			if err != nil {
				log.Panicln("failed to marshal message:", err)
			}

			binary.LittleEndian.PutUint32(varint, uint32(len(data)))
			data = append(varint, data...)

			if _, err := io.Copy(conn, bytes.NewBuffer(data)); err != nil {
				log.Panicln("failed to transmit message:", err)
			}

			if message.Type == raftpb.MsgSnap {
				// TODO: Snapshotting may fail, and that has to be reported through this method.
				n.raft.ReportSnapshot(message.To, raft.SnapshotFinish)
			}
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
		log.Println("accepted connection for", conn.RemoteAddr().String())

		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			defer func() {
				if err := c.Close(); err != nil {
					log.Printf("failed to close connection: %s\n", err)
				}
			}()

			varint := make([]byte, 4)
			r := bufio.NewReader(c)
			for {
				log.Println("receiving message from", c.RemoteAddr().String())
				if _, err := io.ReadFull(r, varint); err != nil {
					log.Panicln("failed to read message varint header into buffer:", err)
				}

				buf := make([]byte, binary.LittleEndian.Uint32(varint))
				if _, err = io.ReadFull(r, buf); err != nil {
					log.Panicln("failed to read message data into buffer:", err)
				}

				var message raftpb.Message
				if err := message.Unmarshal(buf); err != nil {
					log.Panicln("failed to decode raft message:", err)
				}

				if err := n.raft.Step(context.TODO(), message); err != nil {
					log.Panicln("failed to advance raft state machine:", err)
				}
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

func (n *node) Propose(op cluster.Operation) error {
	return n.propose(op)
}

func (n *node) propose(op cluster.Operation) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&op); err != nil {
		log.Panicf("failed to encode operation: %s\n", err)
	}

	errC := n.errors.create(op.ID())
	// Propose blocks until accepted by the cluster.
	// TODO: Find out how to retry proposals.
	err := n.raft.Propose(context.TODO(), buf.Bytes())
	if err != nil {
		return err
	}

	select {
	case <-time.Tick(5 * time.Second):
		panic("timed out")
	case <-errC:
		return err
	}
}

func (n *node) Handle(op cluster.Operation, f cluster.HandlerFunc) {
	t := reflect.TypeOf(op)
	if _, ok := n.handlerFuncs[t]; ok {
		log.Fatalf("operation already registered: %T", op)
	}
	n.handlerFuncs[reflect.TypeOf(op)] = f
	gob.Register(op)
}

func (n *node) process(entry raftpb.Entry) {
	if entry.Data != nil {
		buf := bytes.NewBuffer(entry.Data)

		var op cluster.Operation
		err := gob.NewDecoder(buf).Decode(&op)
		if err != nil {
			log.Panicf("unable to decode as operation: %s", err)
		}

		f, ok := n.handlerFuncs[reflect.TypeOf(op)]
		if !ok {
			log.Panicf("unknown operation received: %T", op)
		}
		err = f(op)

		errC, ok := n.errors.get(op.ID())
		if ok {
			if err != nil {
				errC <- err
			}
			n.errors.delete(op.ID())
		}
	}
}

func (n *node) processSnapshot(snapshot raftpb.Snapshot) {
	log.Printf("Applying snapshot is not implemented yet")
}

type errors struct {
	mu   sync.RWMutex
	errs map[uint64]chan error
}

func (e *errors) create(id uint64) chan error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errs[id] = make(chan error)
	return e.errs[id]
}

func (e *errors) get(id uint64) (chan error, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	err, ok := e.errs[id]
	return err, ok
}

func (e *errors) delete(id uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	close(e.errs[id])
	delete(e.errs, id)
}
