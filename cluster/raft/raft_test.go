package raft

import (
	"testing"
	"time"

	. "github.com/robinkb/cascade-registry/testing"
	"go.etcd.io/raft/v3"
)

func TestRaftClusterFormation(t *testing.T) {
	peers := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	n1 := NewRaftNode(1, peers)
	n2 := NewRaftNode(2, peers)
	n3 := NewRaftNode(3, peers)

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
