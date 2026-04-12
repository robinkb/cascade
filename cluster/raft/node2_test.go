package raft

import (
	"encoding/binary"
	"math/rand/v2"
	"sync"
	"testing"

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

		calls := 100
		s := NewSpyStore(t, node, calls)
		for i := range calls {
			s.Add()
			AssertEqual(t, s.Get(), i)
		}
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
		NewSpyStore(t, node, 0)
		NewSpyStore(t, node, 0)
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

func (s *SpyStore) Verify() {
	AssertEqual(s.t, s.Counter, s.ExpectedCalls).Require()
	AssertEqual(s.t, len(s.State), s.ExpectedCalls).Require()
	if len(s.State) > 0 {
		for i := 0; i < len(s.State); i++ {
			AssertEqual(s.t, s.State[i], i).Require()
		}
	}
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
