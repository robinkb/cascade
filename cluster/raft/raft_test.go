package raft

import (
	"testing"
	"time"

	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
	"go.etcd.io/raft/v3"
)

func TestRaftClusterFormation(t *testing.T) {
	peers := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	n1 := NewRaftNode(1, peers, inmemory.NewMetadataStore())
	n2 := NewRaftNode(2, peers, inmemory.NewMetadataStore())
	n3 := NewRaftNode(3, peers, inmemory.NewMetadataStore())

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
	peers := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	ms1 := inmemory.NewMetadataStore()
	n1 := NewRaftNode(1, peers, ms1)
	ms2 := inmemory.NewMetadataStore()
	n2 := NewRaftNode(2, peers, ms2)
	ms3 := inmemory.NewMetadataStore()
	n3 := NewRaftNode(3, peers, ms3)

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

	time.Sleep(20 * time.Microsecond)

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
