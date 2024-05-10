package controller

import (
	"bytes"
	"testing"
	"time"

	"github.com/distribution/distribution/v3/configuration"
	nats "github.com/nats-io/nats-server/v2/server"
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
	ctl1 := New(dc, &nats.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Port:       -1,
		ServerName: "ctl1",
		Cluster: nats.ClusterOpts{
			Name: "cascade",
			Host: "localhost",
			Port: 6222,
		},
	}, rgc)
	go func() {
		err := ctl1.Start()
		if err != nil {
			t.Error(err)
		}
	}()

	ctl2 := New(dc, &nats.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Port:       -1,
		ServerName: "ctl2",
		Cluster: nats.ClusterOpts{
			Name: "cascade",
			Host: "localhost",
			Port: 6223,
		},
	}, rgc)
	go func() {
		err := ctl2.Start()
		if err != nil {
			t.Error(err)
		}
	}()

	ctl3 := New(dc, &nats.Options{
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Port:       -1,
		ServerName: "ctl3",
		Cluster: nats.ClusterOpts{
			Name: "cascade",
			Host: "localhost",
			Port: 6224,
		},
	}, rgc)
	go func() {
		err := ctl3.Start()
		if err != nil {
			t.Error(err)
		}
	}()

	time.Sleep(15 * time.Second)
	t.Log("first 15 seconds passed")
	time.Sleep(30 * time.Second)
}
