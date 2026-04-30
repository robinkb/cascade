package e2e

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"testing"

	"github.com/robinkb/cascade/cluster/raft/qwal"
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
// BenchmarkRaftCluster/Blob_Size:_1024-8         	     372	   2995130 ns/op	   0.34 MB/s	  634781 B/op	    5416 allocs/op
// BenchmarkRaftCluster/Blob_Size:_32768-8        	     301	   3917251 ns/op	   8.37 MB/s	 2090181 B/op	    5490 allocs/op
// BenchmarkRaftCluster/Blob_Size:_65536-8        	     250	   4500532 ns/op	  14.56 MB/s	 3209947 B/op	    5476 allocs/op
// BenchmarkRaftCluster/Blob_Size:_131072-8       	     198	   5968321 ns/op	  21.96 MB/s	 5784472 B/op	    5499 allocs/op
// BenchmarkRaftCluster/Blob_Size:_262144-8       	     135	   8659361 ns/op	  30.27 MB/s	10315489 B/op	    5493 allocs/op
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
	nodes := NewTestCluster(b, nodeCount, &TestNodeOptions{
		// Large log with many values to eliminate snapshotting and compaction.
		DBOptions: &qwal.Options{
			MaxLogSize:       1 << 30,
			MaxLogValueCount: math.MaxInt64,
		},
	})
	leader, _ := SnapElections(nodes...)
	client := client.NewForHandler(b, leader.RegistryAPI)

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
