package raft

import (
	"sync"
	"testing"

	. "github.com/robinkb/cascade/testing"
)

func TestNodeLifecycle(t *testing.T) {
	t.Run("can start and stop node", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		Run(t, node)

		err := node.Shutdown()
		AssertNoError(t, err)
	})

	t.Run("shutting down twice does not error", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		Run(t, node)

		err := node.Shutdown()
		AssertNoError(t, err)
		err = node.Shutdown()
		AssertNoError(t, err)

	})
}

func TestSingleNode(t *testing.T) {
	t.Run("can form single node cluster", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		Run(t, node)

		snapElections2(node)
		AssertRaftStatus(t, node.Status()).IsLeader().Voters(1)
	})
}

// snapElections rapidly ticks the given nodes until a leader is elected.
func snapElections2(nodes ...Node2) {
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Go(func() {
			for n.Status().Lead == 0 {
				n.Tick()
				wait()
			}
		})
	}

	wg.Wait()
}
