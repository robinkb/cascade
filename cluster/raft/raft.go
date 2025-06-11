package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"math"
	"net/netip"
	"reflect"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/robinkb/cascade-registry/cluster"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func NewNode(id uint64, addr netip.AddrPort, peers []cluster.Peer) cluster.Node {
	storage := raft.NewMemoryStorage()
	conf := raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint16,
		MaxInflightMsgs: 256,
	}

	raftPeers := make([]raft.Peer, len(peers))
	for i := range peers {
		raftPeers[i] = raft.Peer{ID: peers[i].ID}
	}

	transport := cluster.NewTransport(id, addr)

	node := &node{
		id:         id,
		raft:       raft.StartNode(&conf, raftPeers),
		storage:    storage,
		ticker:     time.Tick(1 * time.Second),
		manualTick: make(chan time.Time),
		done:       make(chan struct{}),
		errors: errors{
			errs: make(map[uint64]chan error),
		},

		transport: transport,
		peers:     peers,

		handlerFuncs: make(map[reflect.Type]cluster.HandlerFunc),
	}

	return node
}

type node struct {
	id         uint64
	raft       raft.Node
	ticker     <-chan time.Time
	manualTick chan time.Time
	storage    *raft.MemoryStorage
	done       chan struct{}
	errors     errors

	transport cluster.Transport
	peers     []cluster.Peer

	handlerFuncs map[reflect.Type]cluster.HandlerFunc
}

func (n *node) Start() {
	if err := n.transport.Listen(); err != nil {
		log.Panicln("failed to start transport listener:", err)
	}
	go n.receive()

	for _, peer := range n.peers {
		if n.id == peer.ID {
			continue
		}
		go func() {
			err := n.transport.Add(peer)
			if err != nil {
				log.Panicln("unable to add peer:", err)
			}
		}()
	}
	go n.run()
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
			n.send(rd.Messages)
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

func (n *node) send(messages []raftpb.Message) {
	for _, message := range messages {
		data, err := proto.Marshal(&message)
		if err != nil {
			log.Panicln("failed to marshal message:", err)
		}

		err = n.transport.Send(message.To, data)
		if err != nil {
			log.Panicln("failed to send message:", err)
		}

		if message.Type == raftpb.MsgSnap {
			// TODO: Snapshotting may fail, and that has to be reported through this method.
			n.raft.ReportSnapshot(message.To, raft.SnapshotFinish)
		}
	}
}

func (n *node) receive() {
	for m := range n.transport.Receive() {
		if m.Error != nil {
			log.Println("error received from transport; dropping message:", m.Error)
			continue
		}

		var message raftpb.Message
		if err := message.Unmarshal(m.Data); err != nil {
			log.Panicln("failed to decode raft message:", err)
		}

		if err := n.raft.Step(context.TODO(), message); err != nil {
			log.Panicln("failed to advance raft state machine:", err)
		}
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
