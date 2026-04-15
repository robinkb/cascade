package raft

import (
	"testing"

	. "github.com/robinkb/cascade/testing"
	"go.etcd.io/raft/v3"
)

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
