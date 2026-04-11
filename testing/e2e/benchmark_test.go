package e2e

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/raft"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"github.com/robinkb/cascade/process"
	"github.com/robinkb/cascade/registry"
	registryapi "github.com/robinkb/cascade/registry/api/v2"
	storeapi "github.com/robinkb/cascade/registry/store/api"
	clusterstore "github.com/robinkb/cascade/registry/store/driver/cluster"
	"github.com/robinkb/cascade/registry/store/driver/inmemory"
	"github.com/robinkb/cascade/server"
	. "github.com/robinkb/cascade/testing"
	"github.com/robinkb/cascade/testing/client"
)

type instanceList []instance

func (l *instanceList) nodes() []raft.Node {
	instances := *l
	nodes := make([]raft.Node, len(instances))
	for i, instance := range instances {
		nodes[i] = instance.node
	}
	return nodes
}

type instance struct {
	node     raft.Node
	registry http.Handler
}

func BenchmarkRaftCluster(b *testing.B) {
	tc := []struct {
		blobSize int64
	}{
		{256}, {1 << 10}, {32 << 10}, {64 << 10}, {128 << 10},
	}

	nodeCount := 3

	peers := make([]cluster.Peer, nodeCount)
	for i := range peers {
		peers[i] = cluster.Peer{
			ID:   rand.Uint64(),
			Host: RandomHost(),
		}
	}

	instances := make(instanceList, nodeCount)
	for i, peer := range peers {
		mgr := process.NewManager()
		metadata := inmemory.NewMetadataStore()
		blobs := inmemory.NewBlobStore()

		raftServer := server.New(server.Options{
			Name: "cluster-server",
			Addr: peer.Host,
		})

		db, err := qwal.Open(b.TempDir(), &qwal.Options{
			// Disables cutting and snapshotting for this test.
			// Large log size == No cuts, no snapshots.
			MaxLogSize: 1 << 30,
		})
		AssertNoError(b, err).Require()
		storage, err := raft.NewDiskStorage(db, nil)
		AssertNoError(b, err).Require()

		node := raft.NewNode(peer.ID, peer.Host, storage, nil)
		node.Bootstrap(peers...)

		raftServer.Handle("/cluster/raft/", node.Handler())
		raftServer.Handle("/store/", storeapi.New(blobs))
		mgr.Register(raftServer)
		mgr.Register(node)

		metadata = clusterstore.NewMetadataStore(node, metadata)
		blobs = clusterstore.NewBlobStore(node, blobs)

		registry := registry.New(metadata, blobs)
		registryapi := registryapi.New(registry)

		Run(b, mgr)
		instances[i] = instance{
			node:     node,
			registry: registryapi,
		}
	}

	snapElections(instances.nodes()...)

	var leader instance
	for _, instance := range instances {
		if instance.node.Status().RaftState.String() == "StateLeader" {
			leader = instance
			break
		}
	}

	client := client.NewForHandler(b, leader.registry)

	// What do you mean, "that was a lot of work"?

	for _, tt := range tc {
		b.Run(fmt.Sprintf("Blob Size: %d", tt.blobSize), func(b *testing.B) {
			name := RandomName()
			for b.Loop() {
				id, content := RandomBlob(tt.blobSize)
				b.SetBytes(tt.blobSize)

				resp := client.InitUpload(name)
				AssertResponseCode(b, resp, http.StatusAccepted).Require()
				location, err := resp.Location()
				AssertNoError(b, err).Require()
				resp = client.UploadBlobStream(location, bytes.NewBuffer(content))
				AssertResponseCode(b, resp, http.StatusAccepted)

				resp = client.CloseUpload(location, id)
				AssertResponseCode(b, resp, http.StatusCreated)
			}
		})
	}

}

func Run(tb testing.TB, r *process.Manager) {
	tb.Helper()

	tb.Cleanup(func() {
		err := r.Shutdown()
		AssertNoError(tb, err)
	})

	go func() {
		err := r.Run()
		AssertNoError(tb, err)
	}()
}

// snapElections rapidly ticks the given nodes until a leader is elected.
func snapElections(nodes ...raft.Node) {
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
