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

// BenchmarkRaftCluster provisions a cluster of 3 Raft nodes, each paired with a Registry service.
// A client uploads blobs of random data to the leader. This benchmark is decent for measuring the overhead
// of the clustering protocol. Smaller messages will suffer more.
//
// Before Raft node refactor, with gob encoder:
// goos: linux
// goarch: amd64
// pkg: github.com/robinkb/cascade/testing/e2e
// cpu: 13th Gen Intel(R) Core(TM) i3-1315U
// BenchmarkRaftCluster/Blob_Size:_1024-8         	     164	   7543619 ns/op	   0.14 MB/s	  826087 B/op	    9773 allocs/op
// BenchmarkRaftCluster/Blob_Size:_32768-8        	     126	   9866922 ns/op	   3.32 MB/s	 2505483 B/op	    9931 allocs/op
// BenchmarkRaftCluster/Blob_Size:_65536-8        	     118	  10303460 ns/op	   6.36 MB/s	 3609287 B/op	    9925 allocs/op
// BenchmarkRaftCluster/Blob_Size:_131072-8       	     102	  11166146 ns/op	  11.74 MB/s	 6169218 B/op	    9915 allocs/op
// BenchmarkRaftCluster/Blob_Size:_262144-8       	      91	  13383974 ns/op	  19.59 MB/s	10696418 B/op	    9933 allocs/op
//
// After Raft node refactor, with JSON encoder in registry/store/driver/cluster:
// goos: linux
// goarch: amd64
// pkg: github.com/robinkb/cascade/testing/e2e
// cpu: 13th Gen Intel(R) Core(TM) i3-1315U
// BenchmarkRaftCluster/Blob_Size:_1024-8         	     378	   2860937 ns/op	   0.36 MB/s	  625900 B/op	    5415 allocs/op
// BenchmarkRaftCluster/Blob_Size:_32768-8        	     298	   3984928 ns/op	   8.22 MB/s	 2087198 B/op	    5492 allocs/op
// BenchmarkRaftCluster/Blob_Size:_65536-8        	     100	  14368713 ns/op	   4.56 MB/s	 3261785 B/op	    5500 allocs/op
// BenchmarkRaftCluster/Blob_Size:_131072-8       	     192	   6329394 ns/op	  20.71 MB/s	 5780270 B/op	    5499 allocs/op
// BenchmarkRaftCluster/Blob_Size:_262144-8       	     134	   9077169 ns/op	  28.88 MB/s	10403122 B/op	    5510 allocs/op
func BenchmarkRaftCluster(b *testing.B) {
	tc := []struct {
		blobSize int64
	}{
		{1 << 10},
		{32 << 10},
		{64 << 10},
		{128 << 10},
		{256 << 10},
	}

	nodeCount := 3

	peers := make([]cluster.Peer, nodeCount)
	for i := range peers {
		peers[i] = cluster.Peer{
			ID:   rand.Uint64(),
			Addr: RandomHost(),
		}
	}

	instances := make(instanceList, nodeCount)
	for i, peer := range peers {
		mgr := process.NewManager()
		metadata := inmemory.NewMetadataStore()
		blobs := inmemory.NewBlobStore()

		raftServer := server.New(server.Options{
			Name: "cluster-server",
			Addr: peer.Addr,
		})

		db, err := qwal.Open(b.TempDir(), &qwal.Options{
			// Disables cutting and snapshotting for this test.
			// Large log size == No cuts, no snapshots.
			MaxLogSize: 2 << 30,
		})
		AssertNoError(b, err).Require()
		storage, err := raft.NewDiskStorage(db, nil)
		AssertNoError(b, err).Require()

		node := raft.NewNode(peer.ID, peer.Addr, storage)

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
		node.Bootstrap(peers...)
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
			id, content := RandomBlob(tt.blobSize)
			b.SetBytes(tt.blobSize)

			for b.Loop() {
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
	time.Sleep(10 * time.Millisecond)
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
