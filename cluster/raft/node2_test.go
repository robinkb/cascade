package raft

import (
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/cluster"
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

	t.Run("can handle proposals", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		Run(t, node)
		snapElections2(node)

		pt := Type(10)
		node.Handle(pt, testProposalFunc)

		id, content := RandomBlob(32)
		resp, err := node.Propose(pt, content)
		AssertNoError(t, err)
		AssertEqual(t, string(resp), id.String())
	})
}

// TestProposer has more in-depth tests and unhappy paths
// for the implementation of the [cluster.Proposer] interface.
func TestProposer(t *testing.T) {
	t.Run("encodes and decodes a proposal", func(t *testing.T) {
		wantID := rand.Uint64()
		wantType := rand.Uint32()
		wantData := RandomBytes(64)
		var gotID uint64
		var gotType uint32
		var gotData []byte

		encoded := encodeProposal(wantID, wantType, wantData)
		gotID, gotType, gotData = decodeProposal(encoded)

		AssertEqual(t, gotID, wantID)
		AssertEqual(t, gotType, wantType)
		AssertSlicesEqual(t, gotData, wantData)
	})

	t.Run("registering function for the same type twice panics", func(t *testing.T) {
		defer AssertPanics(t, cluster.ErrDuplicateProposalType)

		node := NewNode2(t.TempDir())
		pt := Type(10)
		node.Handle(pt, testProposalFunc)
		node.Handle(pt, testProposalFunc)
	})

	t.Run("proposing with an unregistered type panics", func(t *testing.T) {
		t.Skip("cannot assert that a separate go routine panics, but this works")

		node := NewNode2(t.TempDir())
		Run(t, node)
		snapElections2(node)

		pt := Type(10)
		_, _ = node.Propose(pt, RandomBytes(32))
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

func testProposalFunc(data []byte) (resp []byte, err error) {
	id := digest.FromBytes(data)
	return []byte(id.String()), nil
}
