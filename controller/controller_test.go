package controller

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/nats-io/nats-server/v2/server"
)

// I swear that this is the easiest way to do it.
const registryConf = `
version: 0.1
storage:
  nats: {}
`

func TestClusterFormation(t *testing.T) {
	rgc, err := configuration.Parse(bytes.NewBufferString(registryConf))
	if err != nil {
		t.Error(err)
	}

	dc := NewDiscoveryClient()
	controllers := []*controller{}

	// Initialize the controllers
	for i := 0; i < 3; i++ {
		controllers = append(controllers, NewController(dc, &server.Options{
			JetStream:  true,
			StoreDir:   t.TempDir(),
			Port:       -1,
			ServerName: fmt.Sprintf("n%d", i),
			Cluster: server.ClusterOpts{
				Name: "cascade",
				Host: "localhost",
				Port: 6222 + i,
			},
		}, rgc))
	}

	// Start all of them
	for _, c := range controllers {
		t.Logf("starting %s", c.nso.ServerName)
		c.Run()
	}

	// Wait for all NATS servers to have started.
	// Maybe this should be a StatusNATS call on the controller.
	for _, c := range controllers {
		for {
			if c.ns == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if !c.ns.ReadyForConnections(4 * time.Second) {
				continue
			}

			break
		}
	}

	// Check if all of them are clustered. Not sure if this is a good check.
	for _, c := range controllers {
		if !c.ns.JetStreamIsClustered() {
			t.Error("not clustered")
		}
	}

	// Shut it all down.
	for _, c := range controllers {
		c.Shutdown()
		c.WaitForShutdown()
	}

	t.Log("shutdown complete")
}
