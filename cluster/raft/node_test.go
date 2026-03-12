package raft

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/robinkb/cascade-registry/registry/store"
	storecluster "github.com/robinkb/cascade-registry/registry/store/cluster"
	"github.com/robinkb/cascade-registry/registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
	"go.etcd.io/raft/v3"
)

func TestClusterFormation(t *testing.T) {
	t.Run("Form a single-node cluster", func(t *testing.T) {
		node := newTestNode(t)
		AssertRaftStatus(t, node.Status()).HasNoLeader().IsFollower().Voters(0)

		// We have to add the node to the Raft state so that it can campaign and become the leader.
		node.Bootstrap()
		node.Start()
		snapElections(node)
		// The Raft status now shows the first node as part of the voters.
		AssertRaftStatus(t, node.Status()).IsLeader().Voters(1)
	})

	t.Run("Form and expand a cluster", func(t *testing.T) {
		node1, node2, node3 := newTestNode(t), newTestNode(t), newTestNode(t)

		// Form a single-node cluster first.
		node1.Bootstrap()
		node1.Start()
		snapElections(node1)

		// Now let's add a second node.
		// Adding the leader of the existing cluster is required.
		node2.Bootstrap(node1.AsPeer())
		node2.Start()

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
		node3.Start()

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
		nodes, _, _ := newTestCluster(t, 3)
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
		// The Raft library says that an ID should not be re-used, but it _does_ work.
		nodes, _, _ := newTestCluster(t, 3)
		snapElections(nodes...)
		AssertRaftStatus(t, nodes[0].Status()).Voters(3)

		err := nodes[0].RemoveNode(context.Background(), nodes[2].AsPeer())
		AssertNoError(t, err)
		AssertRaftStatus(t, nodes[0].Status()).Voters(2)

		// A removed node is stopped, so start it again.
		nodes[2].Start()
		err = nodes[0].AddNode(context.Background(), nodes[2].AsPeer())
		wait()
		AssertNoError(t, err)
		AssertRaftStatus(t, nodes[0].Status()).Voters(3)
	})
}

func TestBlobReplication(t *testing.T) {
	t.Parallel()
	nodes, blobs, _ := newTestCluster(t, 3)
	snapElections(nodes...)

	t.Run("Ensure blobs are replicated", func(t *testing.T) {
		id, content := RandomDigest(), RandomBytes(32)
		err := blobs[0].PutBlob(id, content)
		RequireNoError(t, err)

		wait()

		for _, b := range blobs {
			info, err := b.StatBlob(id)
			AssertNoError(t, err)
			if info != nil {
				AssertEqual(t, info.Size, int64(len(content)))
			}
		}

		err = blobs[0].DeleteBlob(id)
		RequireNoError(t, err)

		wait()

		for _, b := range blobs {
			_, err := b.StatBlob(id)
			AssertErrorIs(t, err, store.ErrNotFound)
		}
	})

	t.Run("Ensure uploads are replicated", func(t *testing.T) {
		id, digest, content := RandomUUID(), RandomDigest(), RandomBytes(32)
		err := blobs[0].InitUpload(id)
		RequireNoError(t, err)

		wait()

		for _, b := range blobs {
			_, err := b.StatUpload(id)
			AssertNoError(t, err)
		}

		w, err := blobs[0].UploadWriter(id)
		RequireNoError(t, err)

		_, err = io.Copy(w, bytes.NewBuffer(content))
		RequireNoError(t, err)

		err = w.Close()
		RequireNoError(t, err)

		err = blobs[0].CloseUpload(id, digest)
		RequireNoError(t, err)

		wait()

		for _, b := range blobs {
			got, err := b.GetBlob(digest)
			AssertNoError(t, err)
			AssertSlicesEqual(t, got, content)
		}
	})

	t.Run("Ensure upload deletions are replicated", func(t *testing.T) {
		id := RandomUUID()
		err := blobs[0].InitUpload(id)
		RequireNoError(t, err)

		wait()

		for _, b := range blobs {
			_, err := b.StatUpload(id)
			AssertNoError(t, err)
		}

		err = blobs[0].DeleteUpload(id)
		RequireNoError(t, err)

		wait()

		for _, b := range blobs {
			_, err := b.StatUpload(id)
			AssertErrorIs(t, err, store.ErrNotFound)
		}
	})
}

func TestMetadataReplication(t *testing.T) {
	t.Parallel()
	nodes, _, metadata := newTestCluster(t, 3)
	snapElections(nodes...)

	t.Run("Ensure repository metadata is replicated", func(t *testing.T) {
		name := RandomName()
		err := metadata[0].CreateRepository(name)
		RequireNoError(t, err)

		wait()

		for _, m := range metadata {
			err := m.GetRepository(name)
			AssertNoError(t, err)
		}

		err = metadata[0].DeleteRepository(name)
		RequireNoError(t, err)

		wait()

		for _, m := range metadata {
			err := m.GetRepository(name)
			AssertErrorIs(t, err, store.ErrRepositoryNotFound)
		}
	})

	t.Run("Ensure blob metadata is replicated", func(t *testing.T) {
		name, digest := RandomName(), RandomDigest()
		err := metadata[0].CreateRepository(name)
		RequireNoError(t, err)
		err = metadata[0].PutBlob(name, digest)
		RequireNoError(t, err)

		wait()

		for _, m := range metadata {
			_, err := m.GetBlob(name, digest)
			AssertNoError(t, err)
		}

		err = metadata[0].DeleteBlob(name, digest)
		RequireNoError(t, err)

		wait()

		for _, m := range metadata {
			_, err := m.GetBlob(name, digest)
			AssertErrorIs(t, err, store.ErrNotFound)
		}
	})

	t.Run("Ensure manifest metadata is replicated", func(t *testing.T) {
		name := RandomName()
		err := metadata[0].CreateRepository(name)
		RequireNoError(t, err)

		digest, manifest, content := RandomManifest()
		meta := &store.ManifestMetadata{
			Annotations:  manifest.Annotations,
			ArtifactType: manifest.ArtifactType,
			MediaType:    manifest.MediaType,
			Size:         int64(len(content)),
		}
		err = metadata[0].PutManifest(name, digest, meta)
		RequireNoError(t, err)

		wait()

		for _, s := range metadata {
			got, err := s.GetManifest(name, digest)
			AssertNoError(t, err)
			AssertDeepEqual(t, got, meta)
		}

		err = metadata[0].DeleteManifest(name, digest)
		RequireNoError(t, err)

		wait()

		for _, s := range metadata {
			_, err := s.GetManifest(name, digest)
			AssertErrorIs(t, err, store.ErrMetadataNotFound)
		}
	})

	t.Run("Ensure tag metadata is replicated", func(t *testing.T) {
		name, tag, digest := RandomName(), RandomVersion(), RandomDigest()
		err := metadata[0].CreateRepository(name)
		RequireNoError(t, err)

		err = metadata[0].PutTag(name, tag, digest)
		RequireNoError(t, err)

		wait()

		for _, s := range metadata {
			got, err := s.GetTag(name, tag)
			AssertNoError(t, err)
			AssertEqual(t, got, digest)
		}

		err = metadata[0].DeleteTag(name, tag)
		RequireNoError(t, err)

		wait()

		for _, s := range metadata {
			_, err := s.GetTag(name, tag)
			AssertErrorIs(t, err, store.ErrNotFound)
		}
	})

	t.Run("Ensure upload session metadata is replicated", func(t *testing.T) {
		name := RandomName()
		err := metadata[0].CreateRepository(name)
		RequireNoError(t, err)

		id, _ := uuid.NewV7()
		session := &store.UploadSession{ID: id}

		err = metadata[0].PutUploadSession(name, session)
		RequireNoError(t, err)

		wait()

		for _, s := range metadata {
			got, err := s.GetUploadSession(name, id.String())
			AssertNoError(t, err)
			AssertDeepEqual(t, got, session)
		}

		err = metadata[0].DeleteUploadSession(name, id.String())
		RequireNoError(t, err)

		wait()

		for _, s := range metadata {
			_, err := s.GetUploadSession(name, id.String())
			AssertErrorIs(t, err, store.ErrNotFound)
		}
	})
}

func newTestNode(t *testing.T) Node {
	return NewNode(rand.Uint64(), RandomAddrPort(), newTestStore(t), &SpySnapshotter{})
}

func newTestCluster(t *testing.T, n int) ([]Node, []store.Blobs, []store.Metadata) {
	peers := make([]Peer, n)
	nodes := make([]Node, n)
	blobs := make([]store.Blobs, n)
	metadata := make([]store.Metadata, n)

	for i := range n {
		peers[i] = Peer{
			ID:       rand.Uint64(),
			AddrPort: RandomAddrPort(),
		}
	}

	for i := range n {
		nodes[i] = NewNode(
			peers[i].ID,
			peers[i].AddrPort,
			newTestStore(t),
			new(SpySnapshotter),
		)
		nodes[i].(*node).Bootstrap(peers...)
		blobs[i] = storecluster.NewBlobStore(nodes[i], inmemory.NewBlobStore())
		metadata[i] = storecluster.NewMetadataStore(nodes[i], inmemory.NewMetadataStore())
		nodes[i].Start()
	}

	return nodes, blobs, metadata
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

func AssertRaftStatus(t *testing.T, status raft.Status) *RaftStatusAsserter {
	t.Helper()
	return &RaftStatusAsserter{t, status}
}

type RaftStatusAsserter struct {
	t      *testing.T
	status raft.Status
}

// Leader asserts that the node is the cluster's leader.
func (a *RaftStatusAsserter) Leader(id uint64) *RaftStatusAsserter {
	a.t.Helper()
	got := a.status.Lead
	if got != id {
		a.t.Logf("unexpected leader id; got %d, want %d", got, id)
		a.t.Fail()
	}
	return a
}

// HasNoLeader asserts that there is no leader in the cluster.
func (a *RaftStatusAsserter) HasNoLeader() *RaftStatusAsserter {
	a.t.Helper()
	got := a.status.Lead
	if a.status.Lead != 0 {
		a.t.Logf("expected leaderless raft; got leader with id %d", got)
		a.t.Fail()
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
	got := a.status.RaftState.String()
	if got != state {
		a.t.Logf("unexpected node state; got %s, want %s", got, state)
		a.t.Fail()
	}
	return a
}

// Voters asserts that there are n voters (members) in the cluster.
func (a *RaftStatusAsserter) Voters(n int) *RaftStatusAsserter {
	a.t.Helper()
	got := len(a.status.Config.Voters.IDs())
	if got != n {
		a.t.Logf("unexpected voter count: got %d, want %d", got, n)
		a.t.Fail()
	}
	return a
}
