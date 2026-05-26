package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/robinkb/cascade/cluster"
)

// AsPeer returns a [cluster.Peer] representing this node.
func (n *node) AsPeer() cluster.Peer {
	return cluster.Peer{
		ID:   n.conf.ID,
		Addr: n.addr,
	}
}

// Bootstrap prepares a new Raft node. If this node will join a cluster,
// at least the cluster's leader must be passed as a peer, but it is safer
// to pass all known peers. Bootstrap effectively bypasses the proposal stage
// and adds Peers directly into the Raft node's state.
func (n *node) Bootstrap(peers ...cluster.Peer) {
	for _, peer := range peers {
		cs := n.raft.ApplyConfChange((&raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode.Enum(),
			NodeId:  &peer.ID,
			Context: []byte(peer.Addr),
		}).AsV2())
		n.storage.SetConfState(cs)

		if peer.ID != n.conf.ID {
			baseUrl := fmt.Sprintf("http://%s/cluster/raft", peer.Addr)
			client := NewClient(baseUrl)
			peer := cluster.Peer{ID: peer.ID, Addr: peer.Addr}
			if err := n.clients.Add(peer, client); err != nil {
				log.Fatal(err)
			}
		}
	}
}

// ConfChangeContext is used as the context data for a Raft ConfChange.
type ConfChangeContext struct {
	// ID is a unique identifier for tracking the ConfChange across the network.
	ID uint64
	// Nodes contains node-specific context index by node ID.
	Nodes map[uint64]string
}

func (c *ConfChangeContext) MustMarshal() []byte {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return data
}

// AddPeer proposes adding the given Peer to the cluster.
// It may be called on any node, not just the leader.
// Proposals may be rejected by the cluster.
func (n *node) AddPeer(p cluster.Peer) error {
	return n.proposeConfChange(raftpb.ConfChangeAddNode, p)
}

// RemovePeer proposes removing the given Peer from the cluster.
// It may be called on any node, not just the leader.
// Proposals may be rejected by the cluster.
func (n *node) RemovePeer(p cluster.Peer) error {
	return n.proposeConfChange(raftpb.ConfChangeRemoveNode, p)
}

func (n *node) proposeConfChange(cct raftpb.ConfChangeType, p cluster.Peer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	id, await := n.confChangeReporter.Await()
	defer n.confChangeReporter.Close(id)

	ccc := &ConfChangeContext{
		ID:    id,
		Nodes: map[uint64]string{p.ID: p.Addr},
	}
	err := n.raft.ProposeConfChange(ctx, (&raftpb.ConfChange{
		Type:    cct.Enum(),
		NodeId:  &p.ID,
		Context: ccc.MustMarshal(),
	}).AsV2())
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case err := <-await:
		return err
	}
}

func (n *node) applyConfChange(cc *raftpb.ConfChangeV2) {
	var ccc ConfChangeContext
	err := json.Unmarshal(cc.Context, &ccc)
	if err != nil {
		panic(err)
	}

	for _, change := range cc.Changes {
		switch change.GetType() {
		case raftpb.ConfChangeAddNode:
			addr, ok := ccc.Nodes[change.GetNodeId()]
			if !ok {
				log.Panicf("node ID not found in conf change context: %x", change.GetNodeId())
			}
			baseUrl := fmt.Sprintf("http://%s/cluster/raft", addr)
			client := NewClient(baseUrl)
			peer := cluster.Peer{ID: change.GetNodeId(), Addr: addr}
			if err := n.clients.Add(peer, client); err != nil {
				log.Fatal(err)
			}
		case raftpb.ConfChangeRemoveNode:
			if change.GetNodeId() == n.conf.ID {
				go n.shutdown()
			} else {
				n.clients.Remove(change.GetNodeId())
			}
		}
	}

	cs := n.raft.ApplyConfChange(cc)
	n.storage.SetConfState(cs)

	if ch, ok := n.confChangeReporter.Send(ccc.ID); ok {
		ch <- nil
	}
}
