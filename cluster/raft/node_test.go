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
	// It doesn't have to be the leader node. Followers can also propose conf changes.
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
	//
	// The problem is now figuring out which node is the first and needs to bootstrap the cluster.
	// In practice, each node can just register themselves in service discovery, and just bootstrap
	// the cluster if there are no other nodes. But this is a potential race condition. Two nodes
	// could come online and bootstrap a cluster at the same time, leaving us with two leaders.
	//
	// Actually... Let's see what happens when two leaders try to join each other.
	// Maybe Raft will handle that?
	// See TestBootstrapWithTwoLeaders
	//
	// It doesn't. So let's just go with a locking mechanism.
	// Or actually it kinda does handle it when you use CheckQuorum.
	// But then a 2-node cluster never recovers. A 3-node might, but I didn't test it.
	//
	// Actually, do we need to use ProposeConfChange at all? Can't we rely on service discovery
	// and use ApplyConfChange directly instead? That's what Raft does when you use StartNode.
	// Next test case:
	// 	1. Start up a 3-node cluster.
	// 	2. Remove a node with ApplyConfChange --> Do we still have quorom? What happens in general?
	//  3. Add a node with ApplyConfChange --> Is it propagated?
	// Maybe when we use ApplyConfChange we'll have to rely on service discovery completely
	// to propagate node changes, because there are no ConfChange messages to process.
	// But that would be fine. Actually, it would simplify things a lot.
	// I just need to figure out what happens to the cluster.
	time.Sleep(1 * time.Second)

	// Removing the leader through a proposal on a follower works.
	// secondNode.raft.ProposeConfChange(context.Background(), raftpb.ConfChangeV2{
	// 	Transition: raftpb.ConfChangeTransitionAuto,
	// 	Changes: []raftpb.ConfChangeSingle{
	// 		raftpb.ConfChangeSingle{
	// 			Type:   raftpb.ConfChangeRemoveNode,
	// 			NodeID: firstNode.raft.Status().ID,
	// 		},
	// 	},
	// })

	// This removes the leader from the second node, but the first node is still the leader.
	// secondNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
	// 	Transition: raftpb.ConfChangeTransitionAuto,
	// 	Changes: []raftpb.ConfChangeSingle{
	// 		raftpb.ConfChangeSingle{
	// 			Type:   raftpb.ConfChangeRemoveNode,
	// 			NodeID: firstNode.raft.Status().ID,
	// 		},
	// 	},
	// })

	// If the leader node removes itself through ApplyConfChange, it steps down as a leader.
	// It also loses itself in its local cluster state, but the other nodes still recognize it.
	// It somehow also participates in leader elections.
	firstNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})
	// But if it's removed everywhere through ApplyConfChange, everything works as expected.
	// So I could move adding and removing nodes out of Raft and completely into the controller,
	// relying completely on service discovery. Raft will figure out a leader upon membership changes.
	// That eliminates any need to have synchornization between service discovery and Raft, I think.
	secondNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})
	thirdNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	time.Sleep(10 * time.Second)
}

func TestBootstrapWithTwoLeaders(t *testing.T) {
	firstAddr := netip.MustParseAddrPort("127.0.0.1:50001")
	firstNode := NewNode(1, firstAddr, nil, t.TempDir(), &SpySnapshotter{}).(*node)

	firstNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	secondAddr := netip.MustParseAddrPort("127.0.0.1:50002")
	secondNode := NewNode(2, secondAddr, nil, t.TempDir(), &SpySnapshotter{}).(*node)

	secondNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: secondNode.raft.Status().ID,
			},
		},
	})

	firstNode.Start()
	secondNode.Start()
	firstNode.raft.Campaign(context.Background())
	secondNode.raft.Campaign(context.Background())

	time.Sleep(1 * time.Second)
	// Now we have two leaders.
	// Let's try to join them and see what happens.

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

	// Second node won't recognize the first node.
	time.Sleep(1 * time.Second)

	// Let's try adding the first to the second.
	err = secondNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
		Context: []byte(secondAddr.String()),
	})
	AssertNoError(t, err).Require()

	// Still nothing. We could probably recover from this situation,
	// but Raft won't do it automatically. We could have nodes forget
	// a leader, and start campaigning again or something.
	// But it's better not to be in this situation to begin with.
	//
	// Actually, if CheckQuorum is active, Raft will start another campaign.
	// But it won't elect another leader, probably because there's only 2 nodes.
	time.Sleep(3 * time.Second)

	thirdAddr := netip.MustParseAddrPort("127.0.0.1:50003")
	thirdNode := NewNode(3, thirdAddr, nil, t.TempDir(), &SpySnapshotter{}).(*node)
	thirdNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: thirdNode.raft.Status().ID,
			},
		},
	})
	thirdNode.Start()
	thirdNode.raft.Campaign(context.Background())

	// Now we have three leaders.
	time.Sleep(1 * time.Second)

	// Try adding the second node to the third.
	thirdNode.mesh.SetPeer(secondNode.raft.Status().ID, secondAddr)
	thirdNode.mesh.SetPeer(firstNode.raft.Status().ID, firstAddr)
	err = thirdNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
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

	time.Sleep(10 * time.Second)

	// It's not really clear to me here what's happening. But it feels like syncing service discovery
	// with cluster state will be a challenge? Actually what is the point of using ProposeConfChange?

	fmt.Printf("%d lead: %d\n", firstNode.raft.Status().ID, firstNode.raft.Status().Lead)
	fmt.Printf("%d lead: %d\n", secondNode.raft.Status().ID, secondNode.raft.Status().Lead)
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
