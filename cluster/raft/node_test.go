package raft

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/process"
	"github.com/robinkb/cascade/server"
	. "github.com/robinkb/cascade/testing"
	"go.etcd.io/raft/v3"
)

func TestBLablabla(t *testing.T) {
	t.Run("Form a single-node cluster", func(t *testing.T) {
		node := newTestNode(t)
		AssertRaftStatus(t, node.Status()).HasNoLeader().IsFollower().Voters(0)

		// We have to add the node to the Raft state so that it can campaign and become the leader.
		node.Bootstrap()
		Run(t, node)
		snapElections(node)
		// The Raft status now shows the first node as part of the voters.
		AssertRaftStatus(t, node.Status()).IsLeader().Voters(1)
	})

	t.Run("Form and expand a cluster", func(t *testing.T) {
		node1, node2, node3 := newTestNode(t), newTestNode(t), newTestNode(t)

		// Form a single-node cluster first.
		node1.Bootstrap()
		Run(t, node1)
		snapElections(node1)

		// Now let's add a second node.
		// Adding the leader of the existing cluster is required.
		node2.Bootstrap(node1.AsPeer())
		Run(t, node2)

		// Any node in the existing cluster can propose to add a node.
		err := node1.AddNode(context.Background(), node2.AsPeer())
		AssertNoError(t, err).Require()
		AssertRaftStatus(t, node1.Status()).Voters(2).IsLeader()
		AssertRaftStatus(t, node2.Status()).Voters(2).IsFollower()

		// Let's add the third, passing all known peers. Adding just the leader
		// would be enough, but it's safer to add them all. It's even possible to
		// bootstrap with only a follower node. But once the new node joins, the leader
		// will not broadcast itself to the new node. The leader must be bootstrapped in.
		node3.Bootstrap(node1.AsPeer(), node2.AsPeer())
		Run(t, node3)

		// And after this, we have three nodes in the cluster.
		err = node2.AddNode(context.Background(), node3.AsPeer())
		AssertNoError(t, err).Require()
		AssertRaftStatus(t, node1.Status()).Voters(3).IsLeader()
		AssertRaftStatus(t, node2.Status()).Voters(3).IsFollower()
		AssertRaftStatus(t, node3.Status()).Voters(3).IsFollower()
	})

	t.Run("Remove a node from a cluster", func(t *testing.T) {
		// At this point we've verified the details of cluster formation,
		// so we can automate it with newTestCluster and snapElections.
		nodes := newTestCluster(t, 3)
		snapElections(nodes...)
		AssertRaftStatus(t, nodes[0].Status()).Voters(3)

		err := nodes[0].RemoveNode(context.Background(), nodes[2].AsPeer())
		AssertNoError(t, err)

		wait()
		AssertRaftStatus(t, nodes[0].Status()).Voters(2)
		AssertRaftStatus(t, nodes[1].Status()).Voters(2)
		// Status returning 0 voters (kinda) indicates that it's stopped.
		AssertRaftStatus(t, nodes[2].Status()).Voters(0)
	})

	t.Run("Remove and rejoin a node with the same ID", func(t *testing.T) {
		t.Skip("test is flaky")

		// The Raft library says that an ID should not be re-used, but it _does_ work.
		nodes := newTestCluster(t, 3)
		snapElections(nodes...)
		AssertRaftStatus(t, nodes[0].Status()).Voters(3)

		err := nodes[0].RemoveNode(context.Background(), nodes[2].AsPeer())
		AssertNoError(t, err)
		AssertRaftStatus(t, nodes[0].Status()).Voters(2)

		// A removed node is stopped, so start it again.
		Run(t, nodes[2])
		err = nodes[0].AddNode(context.Background(), nodes[2].AsPeer())
		wait()
		AssertNoError(t, err)
		AssertRaftStatus(t, nodes[0].Status()).Voters(3)
	})
}

func newTestNode(t *testing.T) Node {
	addr := RandomHost()
	srv := server.New(server.Options{
		Name: "test-server",
		Addr: addr,
	})
	node := NewNode(rand.Uint64(), addr, newTestStore(t), &SpySnapshotter{})
	srv.Handle("/cluster/raft/", node.Handler())

	go func() {
		err := srv.Run()
		AssertNoError(t, err).Require()
	}()

	t.Cleanup(func() {
		err := srv.Shutdown()
		AssertNoError(t, err).Require()
		err = node.Shutdown()
		AssertNoError(t, err).Require()
	})

	return node
}

func newTestCluster(t *testing.T, n int) []Node {
	peers := make([]cluster.Peer, n)
	nodes := make([]Node, n)

	for i := range n {
		peers[i] = cluster.Peer{
			ID:   rand.Uint64(),
			Addr: RandomHost(),
		}
	}

	for i := range n {
		srv := server.New(server.Options{
			Name: fmt.Sprintf("test-server %d", peers[i].ID),
			Addr: peers[i].Addr,
		})
		nodes[i] = NewNode(
			peers[i].ID,
			peers[i].Addr,
			newTestStore(t),
			new(SpySnapshotter),
		)

		srv.Handle("/cluster/raft/", nodes[i].Handler())
		Run(t, srv)

		nodes[i].Bootstrap(peers...)
		Run(t, nodes[i])
	}

	return nodes
}

// snapElections rapidly ticks the given nodes until a leader is elected.
func snapElections(nodes ...Node) {
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

// wait is used for waiting between ticks for Raft test cluster formation and state checks.
// If tests that use wait() are timing out, the sleep interval likely needs to be _increased_.
// Because if Raft ticks too quickly, the cluster will keep failing to elect a leader.
func wait() {
	time.Sleep(6 * time.Millisecond)
}

func AssertRaftStatus(t *testing.T, got raft.Status) *RaftStatusAsserter {
	t.Helper()
	return &RaftStatusAsserter{t, got}
}

type RaftStatusAsserter struct {
	t   *testing.T
	got raft.Status
}

// Equal attempts to compare two Raft statuses.
func (a *RaftStatusAsserter) Equal(want raft.Status) *RaftStatusAsserter {
	AssertEqual(a.t, a.got.ID, want.ID)
	AssertEqual(a.t, a.got.Term, want.Term)
	AssertEqual(a.t, a.got.Vote, want.Vote)
	AssertEqual(a.t, a.got.Commit, want.Commit)
	// Comparing the AppliedIndex is flaky in some tests.
	// Maybe those tests can be adjusted, but the assertion
	// is disabled for now.
	// AssertEqual(a.t, a.got.Applied, want.Applied)
	return a
}

// Leader asserts that the node is the cluster's leader.
func (a *RaftStatusAsserter) Leader(id uint64) *RaftStatusAsserter {
	a.t.Helper()
	got := a.got.Lead
	if got != id {
		a.t.Errorf("unexpected leader id; got %d, want %d", got, id)
	}
	return a
}

// HasNoLeader asserts that there is no leader in the cluster.
func (a *RaftStatusAsserter) HasNoLeader() *RaftStatusAsserter {
	a.t.Helper()
	got := a.got.Lead
	if a.got.Lead != 0 {
		a.t.Errorf("expected leaderless raft; got leader with id %d", got)
	}
	return a
}

// IsLeader asserts that the node is in the leader state.
func (a *RaftStatusAsserter) IsLeader() *RaftStatusAsserter {
	a.t.Helper()
	return a.isState("StateLeader")
}

// IsFollower asserts that the node is in the follower state.
func (a *RaftStatusAsserter) IsFollower() *RaftStatusAsserter {
	a.t.Helper()
	return a.isState("StateFollower")
}

func (a *RaftStatusAsserter) isState(state string) *RaftStatusAsserter {
	a.t.Helper()
	got := a.got.RaftState.String()
	if got != state {
		a.t.Errorf("unexpected node state; got %s, want %s", got, state)
	}
	return a
}

// Voters asserts that there are n voters (members) in the cluster.
func (a *RaftStatusAsserter) Voters(n int) *RaftStatusAsserter {
	a.t.Helper()
	got := len(a.got.Config.Voters.IDs())
	if got != n {
		a.t.Errorf("unexpected voter count: got %d, want %d", got, n)
	}
	return a
}

func (a *RaftStatusAsserter) IsRunning() *RaftStatusAsserter {
	a.t.Helper()
	if a.got.ID == 0 {
		a.t.Error("expected node to be running")
	}
	return a
}

func (a *RaftStatusAsserter) IsStopped() *RaftStatusAsserter {
	a.t.Helper()
	if a.got.ID != 0 {
		a.t.Error("expected node to be stopped")
	}
	return a
}

func Run(t *testing.T, r process.Runnable) {
	t.Helper()

	t.Cleanup(func() {
		err := r.Shutdown()
		AssertNoError(t, err)
	})

	go func() {
		err := r.Run()
		AssertNoError(t, err)
	}()
}
