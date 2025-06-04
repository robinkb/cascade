package raft

import (
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func randomPort() int {
	return rand.IntN(30000) + 1024
}

func TestRaftClusterFormation(t *testing.T) {
	localhost := net.ParseIP("127.0.0.1")
	peers := []Peer{
		{ID: 1, Addr: net.TCPAddr{IP: localhost, Port: randomPort()}},
		{ID: 2, Addr: net.TCPAddr{IP: localhost, Port: randomPort()}},
		{ID: 3, Addr: net.TCPAddr{IP: localhost, Port: randomPort()}},
	}
	n1 := NewRaftNode(1, &peers[0].Addr, peers, inmemory.NewMetadataStore())
	n2 := NewRaftNode(2, &peers[1].Addr, peers, inmemory.NewMetadataStore())
	n3 := NewRaftNode(3, &peers[2].Addr, peers, inmemory.NewMetadataStore())

	AssertEqual(t, n1.ClusterStatus().Clustered, false)
	AssertEqual(t, n2.ClusterStatus().Clustered, false)
	AssertEqual(t, n3.ClusterStatus().Clustered, false)

	n1.Start()
	n2.Start()
	n3.Start()

	time.Sleep(150 * time.Millisecond)

	AssertEqual(t, n1.ClusterStatus().Clustered, true)
	AssertEqual(t, n2.ClusterStatus().Clustered, true)
	AssertEqual(t, n3.ClusterStatus().Clustered, true)
}

func TestRaftClusterReplication(t *testing.T) {
	peers := []Peer{
		{ID: 1, Addr: net.TCPAddr{Port: randomPort()}},
		{ID: 2, Addr: net.TCPAddr{Port: randomPort()}},
		{ID: 3, Addr: net.TCPAddr{Port: randomPort()}},
	}
	ms1 := inmemory.NewMetadataStore()
	n1 := NewRaftNode(1, &peers[0].Addr, peers, ms1)
	ms2 := inmemory.NewMetadataStore()
	n2 := NewRaftNode(2, &peers[1].Addr, peers, ms2)
	ms3 := inmemory.NewMetadataStore()
	n3 := NewRaftNode(3, &peers[2].Addr, peers, ms3)

	n1.Start()
	n2.Start()
	n3.Start()

	// Allow some time for cluster formation.
	time.Sleep(150 * time.Millisecond)

	name, tag, digest := RandomName(), RandomVersion(), RandomDigest()
	ms1.CreateRepository(name)
	ms2.CreateRepository(name)
	ms3.CreateRepository(name)

	n1.PutTag(name, tag, digest)

	time.Sleep(1 * time.Millisecond)

	got, err := n1.GetTag(name, tag)
	AssertNoError(t, err)
	AssertEqual(t, got, digest)

	got, err = n2.GetTag(name, tag)
	AssertNoError(t, err)
	AssertEqual(t, got, digest)

	got, err = n3.GetTag(name, tag)
	AssertNoError(t, err)
	AssertEqual(t, got, digest)
}
