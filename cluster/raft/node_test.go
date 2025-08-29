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

func TestBootstrapCluster(t *testing.T) {
	// First step: Simplify this stupid function so that peers aren't passed statically.
	// first := NewNode(id uint64, addr netip.AddrPort, peers []Peer, workDir string, snap cluster.SnapshotRestorer)
	// Next: Add method to bootstrap the cluster on the first node.
	// Basically making a single-node cluster.
	//
	// Then: Join a node through proposal.
	// Pass the URL in context first.
	// Leave service discovery for later.

	firstAddr := netip.MustParseAddrPort("127.0.0.1:50001")
	firstNode := NewNode(1, firstAddr, nil, t.TempDir(), &SpySnapshotter{}).(*node)
	// blobs := storecluster.NewBlobStore(firstNode, inmemory.NewBlobStore())
	// metadata := storecluster.NewMetadataStore(node, inmemory.NewMetadataStore())

	fmt.Println(firstNode.raft.Status().Config.Voters.IDs()) // map[]

	// We have to add the node to the Raft state so that it can campaign and become the leader.
	// This is pretty much bootstrapping the cluster.
	firstNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	fmt.Println(firstNode.raft.Status().Config.Voters.IDs()) // map[1:{}]

	firstNode.Start()

	// This works; managed to form a 1-node cluster.
	firstNode.raft.Campaign(context.Background())

	secondAddr := netip.MustParseAddrPort("127.0.0.1:50002")
	secondNode := NewNode(2, secondAddr, nil, t.TempDir(), &SpySnapshotter{}).(*node)

	secondNode.Start()

	// Add the first node to the mesh, because it has to send messages to it.
	secondNode.mesh.SetPeer(firstNode.raft.Status().ID, firstAddr)
	// Add the first node to the known nodes.
	secondNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	// Now propose adding the second node to the first node.
	err := firstNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: secondNode.raft.Status().ID,
			},
		},
		Context: []byte(secondAddr.String()),
	})
	AssertNoError(t, err).Require()

	time.Sleep(1 * time.Second)

	// And about now the second node joined.

	// Now let's try adding a third.
	thirdAddr := netip.MustParseAddrPort("127.0.0.1:50003")
	thirdNode := NewNode(3, thirdAddr, nil, t.TempDir(), &SpySnapshotter{}).(*node)

	thirdNode.Start()

	// Let's try adding just the leader
	thirdNode.mesh.SetPeer(firstNode.raft.Status().ID, firstAddr)
	thirdNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	// Now propose adding the third node to the leader node.
	err = firstNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: thirdNode.raft.Status().ID,
			},
		},
		Context: []byte(thirdAddr.String()),
	})
	AssertNoError(t, err).Require()

	// And this is enough! Only the leader needs to be known to the new node.
	// The other nodes get shared over the messages.
	// The context of each node gets saved and shared when new nodes join.
	time.Sleep(5 * time.Second)
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
			t.TempDir(),
			new(SpySnapshotter),
		)
		blobs[i] = storecluster.NewBlobStore(nodes[i], inmemory.NewBlobStore())
		metadata[i] = storecluster.NewMetadataStore(nodes[i], inmemory.NewMetadataStore())
	}

	return nodes, blobs, metadata
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
