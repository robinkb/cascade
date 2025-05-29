package raft

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
)

func TestRaft(t *testing.T) {
	peers := []raft.Peer{{ID: 1}, {ID: 2}}
	nodes[1] = NewRaftNode(1, peers)
	go nodes[1].Run()

	nodes[2] = NewRaftNode(2, peers)
	go nodes[2].Run()

	nodes[1].raft.Campaign(context.TODO())
	time.Sleep(10 * time.Second)
	t.FailNow()
}
