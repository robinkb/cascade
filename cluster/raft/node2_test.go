package raft

import (
	"encoding/binary"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/robinkb/cascade/cluster"
	. "github.com/robinkb/cascade/testing"
)

func TestNodeLifecycle(t *testing.T) {
	t.Run("can start and stop node", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		AssertRaftStatus(t, node.Status()).IsStopped()

		Run2(t, node)
		AssertRaftStatus(t, node.Status()).IsRunning()

		err := node.Shutdown()
		AssertNoError(t, err)
		AssertRaftStatus(t, node.Status()).IsStopped()
	})

	t.Run("shutting down twice does not error", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		Run2(t, node)

		err := node.Shutdown()
		AssertNoError(t, err)
		AssertRaftStatus(t, node.Status()).IsStopped()
		err = node.Shutdown()
		AssertNoError(t, err)
		AssertRaftStatus(t, node.Status()).IsStopped()
	})

	t.Run("shutting down unstarted node does not error", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		err := node.Shutdown()
		AssertNoError(t, err)
		AssertRaftStatus(t, node.Status()).IsStopped()
	})
}

func TestSingleNode(t *testing.T) {
	t.Run("can form single node cluster", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		Run2(t, node)

		SnapElections(node)
		AssertRaftStatus(t, node.Status()).IsLeader().Voters(1)
	})

	t.Run("can handle proposals", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		Run2(t, node)
		SnapElections(node)

		calls := 100
		s := NewSpyStore(t, node, calls)
		for i := range calls {
			s.Add()
			AssertEqual(t, s.Get(), i)
		}
		s.Verify()
	})

	t.Run("retains state after restart", func(t *testing.T) {
		node := NewNode2(t.TempDir())
		Run2(t, node)

		calls := 100
		s := NewSpyStore(t, node, calls)
		for range calls {
			s.Add()
		}

		err := node.Shutdown()
		AssertNoError(t, err)

		Run2(t, node)
		s.Verify()
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
		s := new(SpyStore)
		node.Handle(Type(10), s.add)
		node.Handle(Type(10), s.add)
	})

	t.Run("proposing with an unregistered type panics", func(t *testing.T) {
		t.Skip("cannot assert that a separate go routine panics, but this works")

		node := NewNode2(t.TempDir())
		Run2(t, node)
		SnapElections(node)

		pt := Type(10)
		_, _ = node.Propose(pt, RandomBytes(32))
	})
}

func NewSpyStore(t testing.TB, p Proposer, expectedCalls int) *SpyStore {
	s := &SpyStore{
		t: t,
		p: p,

		ExpectedCalls: expectedCalls,
		State:         make([]int, 0, expectedCalls),
		Type:          Type(rand.Uint32()),
	}

	p.Handle(s.Type, s.add)
	return s
}

type SpyStore struct {
	t testing.TB
	p Proposer

	Type          Type
	ExpectedCalls int
	Counter       int
	State         []int
}

func (s *SpyStore) Add() {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(s.Counter))

	ret, err := s.p.Propose(s.Type, buf)
	AssertNoError(s.t, err).Require()
	AssertEqual(s.t, ret.(int), s.Counter).Require()
	s.Counter++
}

func (s *SpyStore) Get() int {
	return s.State[len(s.State)-1]
}

func (s *SpyStore) add(data []byte) (any, error) {
	n := int(binary.LittleEndian.Uint32(data))
	s.State = append(s.State, n)
	return n, nil
}

// Verify asserts that the SpyStore got exactly the expected commits
// from the Raft state machine. See comments inside for details.
func (s *SpyStore) Verify() {
	// Assert that Add() was called as often as we expected.
	AssertEqual(s.t, s.Counter, s.ExpectedCalls).Require()
	// Assert that we have as many items in the state as was called,
	// indicating that Raft succesfully forwarded all proposals.
	// If we have more items than expected, Raft sent duplicates.
	AssertEqual(s.t, len(s.State), s.ExpectedCalls).Require()
	if len(s.State) > 0 {
		for i := 0; i < len(s.State); i++ {
			// Assert the order of items in the state (0 to ExpectedCalls)
			// to ensure that no messages were delivered out of order.
			AssertEqual(s.t, s.State[i], i).Require()
		}
	}
}

// SnapElections rapidly ticks the given nodes until a leader is elected.
func SnapElections(nodes ...Node2) {
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

// Run2 starts a Node on a Go routine, and blocks until it is started.
// The Node is shut down at the end of the test.
func Run2(t *testing.T, n Node2) {
	t.Helper()

	t.Cleanup(func() {
		err := n.Shutdown()
		AssertNoError(t, err)
	})

	go func() {
		err := n.Run()
		AssertNoError(t, err)
	}()

	for {
		// Effectively waits for the raft node to start.
		if n.Status().Commit != 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
		n.Tick()
	}
}
