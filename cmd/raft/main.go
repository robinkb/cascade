package main

import (
	"time"

	sraft "github.com/robinkb/cascade-registry/store/raft"
	"go.etcd.io/raft/v3"
)

func main() {
	peers := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}
	sraft.Nodes[1] = sraft.NewRaftNode(1, peers)
	go sraft.Nodes[1].Run()

	sraft.Nodes[2] = sraft.NewRaftNode(2, peers)
	go sraft.Nodes[2].Run()

	sraft.Nodes[3] = sraft.NewRaftNode(3, peers)
	go sraft.Nodes[3].Run()

	time.Sleep(30 * time.Second)
}
