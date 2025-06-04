package raft

import (
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/robinkb/cascade-registry/cluster"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

var (
	localhost = net.ParseIP("127.0.0.1")
)

func newTestCluster(n int) []cluster.Node {
	nodes := make([]cluster.Node, n)
	peers := make([]Peer, n)
	for i := range peers {
		peers[i] = Peer{
			ID: rand.Uint64(),
			Addr: net.TCPAddr{
				IP:   localhost,
				Port: randomPort(),
			},
		}
	}

	for i := range nodes {
		nodes[i] = NewRaftNode(
			peers[i].ID,
			&peers[i].Addr,
			peers,
			inmemory.NewMetadataStore(),
		)
	}

	return nodes
}

func randomPort() int {
	return rand.IntN(30000) + 1024
}

func TestRaftClusterFormation(t *testing.T) {
	nodes := newTestCluster(3)

	for _, n := range nodes {
		AssertEqual(t, n.ClusterStatus().Clustered, false)
		n.Start()
	}

	time.Sleep(150 * time.Millisecond)

	for _, n := range nodes {
		AssertEqual(t, n.ClusterStatus().Clustered, true)
	}
}

func TestRaftClusterReplication(t *testing.T) {
	nodes := newTestCluster(3)
	for _, n := range nodes {
		n.Start()
	}

	// Allow some time for cluster formation.
	time.Sleep(150 * time.Millisecond)

	name, tag, digest := RandomName(), RandomVersion(), RandomDigest()
	for _, n := range nodes {
		n.(*node).Metadata.CreateRepository(name)
	}

	err := nodes[0].PutTag(name, tag, digest)
	AssertNoError(t, err)

	time.Sleep(1 * time.Millisecond)

	for _, n := range nodes {
		got, err := n.GetTag(name, tag)
		AssertNoError(t, err)
		AssertEqual(t, got, digest)
	}
}
