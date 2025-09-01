package raft

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/robinkb/cascade-registry/store"
	storecluster "github.com/robinkb/cascade-registry/store/cluster"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
	"go.etcd.io/raft/v3/raftpb"
)

func TestClusterFormation(t *testing.T) {
	t.Run("Form a single-node cluster", func(t *testing.T) {
		addr := RandomAddrPort()
		node := NewNode(rand.Uint64(), addr, nil, testStore(t), &SpySnapshotter{}).(*node)

		AssertRaftStatus(t, node.raft.Status()).
			HasNoLeader().IsFollower().Voters(0)

		node.Bootstrap()

		node.Start()

		snapElections2(node)

		AssertRaftStatus(t, node.raft.Status()).
			IsLeader().Voters(1)
	})

	t.Run("Form and expand a cluster", func(t *testing.T) {
		addr1, addr2, _ := RandomAddrPort(), RandomAddrPort(), RandomAddrPort()

		node1 := NewNode(rand.Uint64(), addr1, nil, testStore(t), &SpySnapshotter{}).(*node)
		node1.Bootstrap()
		node1.Start()
		snapElections2(node1)

		node2 := NewNode(rand.Uint64(), addr2, nil, testStore(t), &SpySnapshotter{}).(*node)
		node2.Start()

		node2.mesh.SetPeer(node1.raft.Status().ID, addr1)
		node2.raft.ApplyConfChange(raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  node1.raft.Status().ID,
			Context: []byte(addr1.String()),
		}.AsV2())

		err := node1.raft.ProposeConfChange(context.Background(), raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  node2.raft.Status().ID,
			Context: []byte(addr2.String()),
		}.AsV2())
		AssertNoError(t, err).Require()

		AssertRaftStatus(t, node1.raft.Status()).
			Voters(2).IsLeader()
		AssertRaftStatus(t, node2.raft.Status()).
			Voters(2).IsFollower()
	})
}

func newTestCluster(t *testing.T, n int) ([]Node, []store.Blobs, []store.Metadata) {
	peers := make([]Peer, n)
	nodes := make([]Node, n)
	blobs := make([]store.Blobs, n)
	metadata := make([]store.Metadata, n)

	for i := range n {
		peers[i] = Peer{
			ID: rand.Uint64(),
			AddrPort: netip.MustParseAddrPort(
				fmt.Sprintf("127.0.0.1:%d", RandomPort()),
			),
		}
	}

	for i := range n {
		nodes[i] = NewNode(
			peers[i].ID,
			peers[i].AddrPort,
			peers,
			testStore(t),
			new(SpySnapshotter),
		)
		blobs[i] = storecluster.NewBlobStore(nodes[i], inmemory.NewBlobStore())
		metadata[i] = storecluster.NewMetadataStore(nodes[i], inmemory.NewMetadataStore())
	}

	return nodes, blobs, metadata
}

func snapElections2(nodes ...Node) {
	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		go func() {
			for n.(*node).raft.Status().Lead == 0 {
				n.Tick()
				wait()
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func snapElections(nodes []Node) {
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		go func() {
			for !n.ClusterStatus().Clustered {
				n.Tick()
				wait()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestRaftClusterFormation(t *testing.T) {
	nodes, _, _ := newTestCluster(t, 3)

	for _, n := range nodes {
		AssertEqual(t, n.ClusterStatus().Clustered, false)
		n.Start()
	}

	snapElections(nodes)

	for _, n := range nodes {
		AssertEqual(t, n.ClusterStatus().Clustered, true)
	}
}

func TestBlobReplication(t *testing.T) {
	t.Parallel()
	nodes, blobs, _ := newTestCluster(t, 3)
	for _, n := range nodes {
		n.Start()
	}
	snapElections(nodes)

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
	for _, n := range nodes {
		n.Start()
	}
	snapElections(nodes)

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

func wait() {
	time.Sleep(3000 * time.Microsecond)
}
