package e2e

import (
	"sync"
	"testing"
	"time"

	"github.com/robinkb/cascade/cluster"
)

func NewTestCluster(t testing.TB, n int, opts *TestNodeOptions) []TestNode {
	nodes := make([]TestNode, n)
	peers := make([]cluster.Peer, n)

	for i := range n {
		nodes[i] = NewTestNode(t, uint64(i+1), opts)
		peers[i] = nodes[i].Node.AsPeer()
	}

	for _, node := range nodes {
		Run(t, node)
		node.Node.Bootstrap(peers...)
	}

	return nodes
}

func SnapElections(nodes ...TestNode) (leader TestNode, followers []TestNode) {
	var wg sync.WaitGroup
	candidates := len(nodes)
	votes := 0

	for _, n := range nodes {
		wg.Go(func() {
			done := false
			for candidates != votes {
				if !done && n.Node.Status().Lead != 0 {
					votes++
					done = true
				}
				n.Node.Tick()
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	wg.Wait()

	for _, n := range nodes {
		if n.Node.Status().RaftState.String() == "StateLeader" {
			leader = n
		} else {
			followers = append(followers, n)
		}
	}

	return
}
