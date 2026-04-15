package raft

import (
	"encoding/binary"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"github.com/robinkb/cascade/server"
	. "github.com/robinkb/cascade/testing"
)

func TestNodeLifecycle(t *testing.T) {
	t.Run("can start and stop node", func(t *testing.T) {
		node := NewTestNode(t)
		AssertRaftStatus(t, node.Status()).IsStopped()

		Run2(t, node)
		AssertRaftStatus(t, node.Status()).IsRunning()

		err := node.Shutdown()
		AssertNoError(t, err)
		AssertRaftStatus(t, node.Status()).IsStopped()
	})

	t.Run("shutting down twice does not error", func(t *testing.T) {
		node := NewTestNode(t)
		Run2(t, node)

		err := node.Shutdown()
		AssertNoError(t, err)
		AssertRaftStatus(t, node.Status()).IsStopped()
		err = node.Shutdown()
		AssertNoError(t, err)
		AssertRaftStatus(t, node.Status()).IsStopped()
	})

	t.Run("shutting down unstarted node does not error", func(t *testing.T) {
		node := NewTestNode(t)
		err := node.Shutdown()
		AssertNoError(t, err)
		AssertRaftStatus(t, node.Status()).IsStopped()
	})
}

func TestSingleNode(t *testing.T) {
	t.Run("can form single node cluster", func(t *testing.T) {
		node := NewTestNode(t)
		Run2(t, node)

		SnapElections(node)
		AssertRaftStatus(t, node.Status()).IsLeader().Voters(1)
	})

	t.Run("can handle proposals", func(t *testing.T) {
		node := NewTestNode(t)
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
		node := NewTestNode(t)
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

	t.Run("restores state from disk", func(t *testing.T) {
		storage := newTestStore(t)

		oldNode := NewNode2(1, "", storage)
		Run2(t, oldNode)

		calls := 100
		s := NewSpyStore(t, oldNode, calls)
		for range calls {
			s.Add()
		}

		oldStatus := oldNode.Status()

		err := oldNode.Shutdown()
		AssertNoError(t, err)

		newNode := NewNode2(1, "", storage)
		Run2(t, newNode)
		s.Verify()
		newStatus := newNode.Status()
		AssertRaftStatus(t, oldStatus).Equal(newStatus)
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

		node := NewTestNode(t)
		s := new(SpyStore)
		node.Handle(Type(10), s.add)
		node.Handle(Type(10), s.add)
	})

	t.Run("proposing with an unregistered type panics", func(t *testing.T) {
		t.Skip("cannot assert that a separate go routine panics, but this works")

		node := NewTestNode(t)
		Run2(t, node)
		SnapElections(node)

		pt := Type(10)
		_, _ = node.Propose(pt, RandomBytes(32))
	})
}

func TestClusterFormation(t *testing.T) {
	t.Run("Form and expand a cluster", func(t *testing.T) {
		node1, node2, node3 := NewTestNode(t), NewTestNode(t), NewTestNode(t)

		// Form a single-node cluster first.
		Run2(t, node1)
		SnapElections(node1)

		// Now let's add a second node.
		// Adding the leader of the existing cluster is required.
		Run2(t, node2)
		node2.Bootstrap(node1.AsPeer())

		// Any node in the existing cluster can propose to add a node.
		err := node1.AddPeer(node2.AsPeer())
		AssertNoError(t, err).Require()
		AssertRaftStatus(t, node1.Status()).Voters(2).IsLeader()
		AssertRaftStatus(t, node2.Status()).Voters(2).IsFollower()

		// Go through snap elections again to ensure that we have a leader.
		SnapElections(node1, node2)
		AssertRaftStatus(t, node1.Status()).Voters(2).IsLeader()
		AssertRaftStatus(t, node2.Status()).Voters(2).IsFollower().Leader(node1.AsPeer().ID)

		// Let's add the third, passing all known peers. Adding just the leader
		// would be enough, but it's safer to add them all. It's even possible to
		// bootstrap with only a follower node. But once the new node joins, the leader
		// will not broadcast itself to the new node. The leader must be bootstrapped in.
		Run2(t, node3)
		node3.Bootstrap(node1.AsPeer(), node2.AsPeer())

		// And after this, we have three nodes in the cluster.
		err = node2.AddPeer(node3.AsPeer())
		AssertNoError(t, err).Require()
		AssertRaftStatus(t, node1.Status()).Voters(3).IsLeader()
		AssertRaftStatus(t, node2.Status()).Voters(3).IsFollower()
		AssertRaftStatus(t, node3.Status()).Voters(3).IsFollower()
	})

	t.Run("Remove a node from a cluster", func(t *testing.T) {
		// At this point we've verified the details of cluster formation,
		// so we can automate it with NewTestCluster and SnapElections.
		nodes := NewTestCluster(t, 3)
		SnapElections(nodes...)
		AssertRaftStatus(t, nodes[0].Status()).Voters(3)

		err := nodes[0].RemovePeer(nodes[2].AsPeer())
		AssertNoError(t, err)

		SnapElections(nodes[0:1]...)
		AssertRaftStatus(t, nodes[0].Status()).Voters(2)
		AssertRaftStatus(t, nodes[1].Status()).Voters(2)
		wait()
		AssertRaftStatus(t, nodes[2].Status()).IsStopped()
	})

	t.Run("Remove and rejoin a node with the same ID", func(t *testing.T) {
		// The Raft library says that an ID should not be re-used, but it _does_ work.
		nodes := NewTestCluster(t, 3)
		SnapElections(nodes...)
		AssertRaftStatus(t, nodes[0].Status()).Voters(3)

		err := nodes[0].RemovePeer(nodes[2].AsPeer())
		AssertNoError(t, err)
		AssertRaftStatus(t, nodes[0].Status()).Voters(2)

		// A removed node is stopped, so start it again.
		Run2(t, nodes[2])
		err = nodes[0].AddPeer(nodes[2].AsPeer())
		wait()
		AssertNoError(t, err)
		AssertRaftStatus(t, nodes[0].Status()).Voters(3)
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
				time.Sleep(5 * time.Millisecond)
			}
		})
	}

	wg.Wait()
}

func NewTestNode(t *testing.T) Node2 {
	dir := t.TempDir()
	db, err := qwal.Open(dir, nil)
	AssertNoError(t, err).Require()

	storage, err := NewDiskStorage(db, nil)
	AssertNoError(t, err).Require()

	id := rand.Uint64N(1000)
	addr := RandomHost()
	node := NewNode2(id, addr, storage)

	srv := server.New(server.Options{
		Name: "raft-server",
		Addr: addr,
	})
	srv.Handle("/cluster/raft/", node.Handler())

	go func() {
		err := srv.Run()
		AssertNoError(t, err).Require()
	}()

	t.Cleanup(func() {
		err := srv.Shutdown()
		AssertNoError(t, err).Require()
	})

	return node
}

// Run2 starts a Node on a Go routine, and blocks until it is started.
// The Node is shut down at the end of the test.
func Run2(t *testing.T, n Node2) {
	t.Helper()

	t.Cleanup(func() {
		err := n.Shutdown()
		AssertNoError(t, err)
	})

	go func(t testing.TB) {
		t.Helper()
		err := n.Run()
		AssertNoError(t, err)
	}(t)

	for {
		// Effectively waits for the Raft node to start.
		// Nodes are not allowed to have ID 0, which is the zero value
		// in the status. If it's not 0, that means that the node has started.
		if n.Status().ID != 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
		n.Tick()
	}
}

func NewTestCluster(t *testing.T, n int) []Node2 {
	nodes := make([]Node2, n)
	peers := make([]cluster.Peer, n)

	for i := range n {
		nodes[i] = NewTestNode(t)
		peers[i] = nodes[i].AsPeer()
	}

	for i := range n {
		Run2(t, nodes[i])
		for j := range n {
			if nodes[i].AsPeer().ID != peers[j].ID {
				nodes[i].Bootstrap(peers[j])
			}
		}
	}

	return nodes
}
