package raft

import (
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/robinkb/cascade-registry/cluster"
	"github.com/robinkb/cascade-registry/store"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

var (
	localhost = net.ParseIP("127.0.0.1")
)

func newTestCluster(n int) []cluster.Node {
	nodes := make([]cluster.Node, n)
	peers := make([]Peer, n)
	for i := range peers {
		peers[i] = Peer{
			ID: rand.Uint64(),
			Addr: net.TCPAddr{
				IP:   localhost,
				Port: randomPort(),
			},
		}
	}

	for i := range nodes {
		nodes[i] = NewRaftNode(
			peers[i].ID,
			&peers[i].Addr,
			peers,
			inmemory.NewMetadataStore(),
		)
	}

	return nodes
}

func randomPort() int {
	return rand.IntN(30000) + 1024
}

func TestRaftClusterFormation(t *testing.T) {
	nodes := newTestCluster(3)

	for _, n := range nodes {
		AssertEqual(t, n.ClusterStatus().Clustered, false)
		n.Start()
	}

	time.Sleep(200 * time.Millisecond)

	for _, n := range nodes {
		AssertEqual(t, n.ClusterStatus().Clustered, true)
	}
}

func TestRaftClusterReplication(t *testing.T) {
	t.Parallel()
	nodes := newTestCluster(3)
	for _, n := range nodes {
		n.Start()
	}

	// Allow some time for cluster formation.
	time.Sleep(200 * time.Millisecond)

	t.Run("Ensure repository metadata is replicated", func(t *testing.T) {
		name := RandomName()
		err := nodes[0].CreateRepository(name)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			err := n.GetRepository(name)
			AssertNoError(t, err)
		}

		err = nodes[0].DeleteRepository(name)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			err := n.GetRepository(name)
			AssertErrorIs(t, err, store.ErrRepositoryNotFound)
		}
	})

	t.Run("Ensure blob metadata is replicated", func(t *testing.T) {
		name, digest := RandomName(), RandomDigest()
		err := nodes[0].CreateRepository(name)
		RequireNoError(t, err)
		err = nodes[0].PutBlob(name, digest)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			_, err := n.GetBlob(name, digest)
			AssertNoError(t, err)
		}

		err = nodes[0].DeleteBlob(name, digest)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			_, err := n.GetBlob(name, digest)
			AssertErrorIs(t, err, store.ErrNotFound)
		}
	})

	t.Run("Ensure manifest metadata is replicated", func(t *testing.T) {
		name := RandomName()
		err := nodes[0].CreateRepository(name)
		RequireNoError(t, err)

		digest, manifest, content := RandomManifest()
		meta := &store.ManifestMetadata{
			Annotations:  manifest.Annotations,
			ArtifactType: manifest.ArtifactType,
			MediaType:    manifest.MediaType,
			Size:         int64(len(content)),
		}
		err = nodes[0].PutManifest(name, digest, meta)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			got, err := n.GetManifest(name, digest)
			AssertNoError(t, err)
			AssertStructsEqual(t, got, meta)
		}

		err = nodes[0].DeleteManifest(name, digest)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			_, err := n.GetManifest(name, digest)
			AssertErrorIs(t, err, store.ErrMetadataNotFound)
		}
	})

	t.Run("Ensure tag metadata is replicated", func(t *testing.T) {
		name, tag, digest := RandomName(), RandomVersion(), RandomDigest()
		err := nodes[0].CreateRepository(name)
		RequireNoError(t, err)

		err = nodes[0].PutTag(name, tag, digest)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			got, err := n.GetTag(name, tag)
			AssertNoError(t, err)
			AssertEqual(t, got, digest)
		}

		err = nodes[0].DeleteTag(name, tag)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			_, err := n.GetTag(name, tag)
			AssertErrorIs(t, err, store.ErrNotFound)
		}
	})

	t.Run("Ensure upload session metadata is replicated", func(t *testing.T) {
		name := RandomName()
		err := nodes[0].CreateRepository(name)
		RequireNoError(t, err)

		id, _ := uuid.NewV7()
		session := &store.UploadSession{ID: id}

		err = nodes[0].PutUploadSession(name, session)
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			got, err := n.GetUploadSession(name, id.String())
			AssertNoError(t, err)
			AssertStructsEqual(t, got, session)
		}

		err = nodes[0].DeleteUploadSession(name, id.String())
		AssertNoError(t, err)

		time.Sleep(1 * time.Millisecond)

		for _, n := range nodes {
			_, err := n.GetUploadSession(name, id.String())
			AssertErrorIs(t, err, store.ErrNotFound)
		}
	})
}
