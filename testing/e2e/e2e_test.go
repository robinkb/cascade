package e2e

import (
	"net/http"
	"testing"

	. "github.com/robinkb/cascade/testing"
	"github.com/robinkb/cascade/testing/client"
)

// TestLinearizableUploads ensures that uploads are linearizable. It simulates a client uploading
// a large blob through a round-robin load balancer by pushing blobs to the followers
// in a Cascade cluster in alternating fashion. If uploads are not linearizable, a client may
// attempt to upload a chunk to follower that is not yet in sync with the rest of the cluster.
// This will result in a 416 Requested Range Not Satisfiable error.
func TestLinearizableUploads(t *testing.T) {
	nodes := NewTestCluster(t, 3, nil)
	leader, followers := SnapElections(nodes...)

	leaderClient := client.NewForHandler(t, leader.RegistryAPI)
	followerClients := make([]client.Client, len(followers))
	for i := range followerClients {
		followerClients[i] = *client.NewForHandler(t, followers[i].RegistryAPI)
	}

	resp := leaderClient.InitUpload(RandomName())
	AssertResponseCode(t, resp, http.StatusAccepted)
	location, err := resp.Location()
	AssertNoError(t, err).Require()

	written := 0
	chunkSize := 256 << 10
	for i := range 100 {
		resp = followerClients[i%2].UploadBlobChunk(location, RandomBytes(int64(chunkSize)), written)
		AssertResponseCode(t, resp, http.StatusAccepted)
		written += chunkSize
	}
}
