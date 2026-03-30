package raft

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/netip"
	"time"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/process"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	Node interface {
		process.Runnable

		Tick()

		NodeID() uint64
		AddrPort() netip.AddrPort
		AsPeer() cluster.Peer
		Bootstrap(peers ...cluster.Peer)
		AddNode(ctx context.Context, peer cluster.Peer) error
		RemoveNode(ctx context.Context, peer cluster.Peer) error
		Status() raft.Status

		// Messaging
		Receive(m *raftpb.Message) error

		cluster.Proposer
	}
)

// TODO: NewNode should return an error instead of panicking? Probably?
func NewNode(id uint64, addr netip.AddrPort, storage *DiskStorage, restorer cluster.Restorer) Node {
	conf := raft.Config{
		// TODO: This may need to be set when restarting a node.
		// But I'm not sure of how to persist it. It can only be saved _after_
		// applying entries to the state machine. And etcd doesn't seem to set this either.
		Applied: 0,

		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,

		// If weird stuff happens, I should check these toggles.
		StepDownOnRemoval: true,
		PreVote:           true,
		CheckQuorum:       true,
	}

	node := &node{
		id:          id,
		addr:        addr,
		raft:        raft.RestartNode(&conf),
		storage:     storage,
		ticker:      time.Tick(100 * time.Millisecond),
		manualTick:  make(chan time.Time),
		confChanges: newErrCs(),
		clients:     cluster.NewClients[Client](),
	}

	node.proposer = newProposer(node.raft)
	node.restorer = restorer

	return node
}

type node struct {
	id         uint64
	addr       netip.AddrPort
	raft       raft.Node
	ticker     <-chan time.Time
	manualTick chan time.Time
	done       chan struct{}

	confChanges errCs

	*proposer
	clients  cluster.Clients[Client]
	storage  *DiskStorage
	restorer cluster.Restorer
}

func (n *node) Name() string {
	return fmt.Sprintf("raft node %d", n.id)
}

func (n *node) Run() error {
	n.done = make(chan struct{}, 1)
	n.run()
	return nil
}

func (n *node) Shutdown() error {
	n.raft.Stop()
	// n.done <- struct{}{}
	// close(n.done)
	return nil
}

func (n *node) NodeID() uint64 {
	return n.id
}

func (n *node) AddrPort() netip.AddrPort {
	return n.addr
}

func (n *node) AsPeer() cluster.Peer {
	return cluster.Peer{
		ID:       n.NodeID(),
		AddrPort: n.AddrPort(),
	}
}

// Bootstrap prepares a new Raft node. If this node will join a cluster,
// at least the cluster's leader must be passed as a peer, but it is safer
// to pass all known peers.
func (n *node) Bootstrap(peers ...cluster.Peer) {
	peers = append(peers, cluster.Peer{
		ID:       n.raft.Status().ID,
		AddrPort: n.addr,
	})

	for _, peer := range peers {
		confState := n.raft.ApplyConfChange(raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  peer.ID,
			Context: []byte(peer.AddrPort.String()),
		}.AsV2())

		n.storage.SaveConfState(*confState)

		if n.id != peer.ID {
			client := NewClient("http://" + peer.AddrPort.String() + "/cluster/raft")
			if err := n.clients.Add(peer, client); err != nil {
				log.Printf("failed to add client for peer with ID %d: %s", peer.ID, err)
			}
		}
	}
}

// AddNode proposes adding the given Peer to the cluster.
// It may be called on any node, not just the leader.
// Proposals may be rejected by the cluster. AddNode blocks
// until the Peer is added, an error is encountered,
// or until the context is cancelled.
// TODO: AddNode should have a timeout attached to it.
// Could generate a context with timeout inside this method.
// Not sure why it should be able to take a context as argument.
func (n *node) AddNode(ctx context.Context, peer cluster.Peer) error {
	return n.proposeConfChange(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  peer.ID,
		Context: []byte(peer.AddrPort.String()),
	})
}

// RemoveNode proposes removing the given Peer from the cluster.
// It may be called on any node, not just the leader.
// Proposals may be rejected by the cluster. RemoveNode blocks
// until the Peer is removed, an error is encountered,
// or until the context is cancelled.
func (n *node) RemoveNode(ctx context.Context, peer cluster.Peer) error {
	return n.proposeConfChange(ctx, raftpb.ConfChange{
		Type:    raftpb.ConfChangeRemoveNode,
		NodeID:  peer.ID,
		Context: []byte(peer.AddrPort.String()),
	})
}

func (n *node) Status() raft.Status {
	return n.raft.Status()
}

func (n *node) proposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	// TODO: Not really safe to use node ID as a key to track individual conf changes.
	// Multiple conf changes about the same node may be underway at once.
	// A unique ID should be generated per conf change instead.
	errC := n.confChanges.create(cc.NodeID)
	defer n.confChanges.delete(cc.NodeID)

	err := n.raft.ProposeConfChange(ctx, cc.AsV2())
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case err := <-errC:
		return err
	}
}

func (n *node) Tick() {
	n.manualTick <- time.Now()
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
		defer func() {
			if r := recover(); r != nil {
				log.Printf("failed to send message to peer %d: %s", message.To, r)
			}
		}()

		client, _ := n.clients.Get(message.To)
		err := client.SendMessage(&message)
		if err != nil {
			log.Println("failed to send message:", err)
		}
	}
}

func (n *node) processSnapshot(snap raftpb.Snapshot) {
	leaderID := n.raft.Status().Lead
	peer, err := n.clients.Peer(leaderID)
	if err != nil {
		log.Panic("failed to find leader in local clients")
	}

	buf := bytes.NewBuffer(snap.Data)
	err = n.restorer.Restore(buf, peer)
	if err != nil {
		log.Printf("failed to restore snapshot: %s", err)
		n.raft.ReportSnapshot(n.id, raft.SnapshotFailure)
		return
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
				n.commit(entry.Data)
			}
		case raftpb.EntryConfChange:
			panic("received EntryConfChange v1; must be v2")
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(entry.Data); err != nil {
				log.Panicf("could not read ConfChange entry: %s", err)
			}

			for _, change := range cc.Changes {
				switch change.Type {
				case raftpb.ConfChangeAddNode:
					// TODO: Adding a client can error out, technically, in which case
					// we should not call raft.ApplyConfChange because it did not get accepted.
					addr := netip.MustParseAddrPort(string(cc.Context))
					client := NewClient("http://" + addr.String() + "/cluster/raft")
					peer := cluster.Peer{ID: change.NodeID, AddrPort: addr}
					if err := n.clients.Add(peer, client); err != nil {
						log.Printf("failed to add client for peer with ID %d: %s", change.NodeID, err)
					}

					log.Printf("%d added node with id %d and url %s", n.NodeID(), change.NodeID, addr.String())

				case raftpb.ConfChangeRemoveNode:
					if change.NodeID == n.NodeID() {
						log.Println("removed from the cluster; stopping...")
						if err := n.Shutdown(); err != nil {
							log.Fatalf("failed to stop node: %s", err)
						}
					} else {
						log.Printf("%d removed node with id %d", n.NodeID(), change.NodeID)
						n.clients.Remove(change.NodeID)
					}
				}

				if errC, ok := n.confChanges.get(change.NodeID); ok {
					errC <- nil
				}
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
