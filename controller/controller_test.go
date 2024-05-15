package controller

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

// 1. Start virtual node with no tags
// 2. Start actual node tagged with "cascade" or something
// 3. Any created streams must select the "cascade" tag for placement
// 4. Second real node joins the cluster
// 5. Virtual node is removed

// Maybe do the following, or maybe leave it:
// * Tag-based placements are removed from streams (can we do this?)
// * Tags are removed from the first actual node

// This works!! 🎉
func TestClusterUpgradeWithVirtualNode(t *testing.T) {
	primeroOpts := &server.Options{
		ServerName: "primero",
		Port:       4222,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Routes: []*url.URL{
			{Host: "localhost:6221"},
		},
		Tags: jwt.TagList{"app:cascade"},
		Cluster: server.ClusterOpts{
			Name: "cascade",
			Host: "localhost",
			Port: 6222,
		},
		// SystemAccount: "$SYS",
		// Accounts: []*server.Account{
		// 	{
		// 		Name: "$SYS",
		// 	},
		// },
		// Users: []*server.User{
		// 	{
		// 		Username: "admin",
		// 		Password: "admin",
		// 		Account:  &server.Account{Name: "$SYS"},
		// 	},
		// },
	}
	primero, err := server.NewServer(primeroOpts)
	if err != nil {
		t.Fatal(err)
	}
	primero.ConfigureLogger()

	if err := server.Run(primero); err != nil {
		t.Fatal(err)
	}

	virtualOpts := &server.Options{
		ServerName: "virtual",
		Port:       4221,
		JetStream:  true,
		// Virtual server must persist cluster info to disk to survive reboots.
		StoreDir: t.TempDir(),
		Routes: []*url.URL{
			{Host: "localhost:6222"},
		},
		Cluster: server.ClusterOpts{
			Name: "cascade",
			Host: "localhost",
			Port: 6221,
		},
		// SystemAccount: "$SYS",
		// Accounts: []*server.Account{
		// 	{
		// 		Name: "$SYS",
		// 	},
		// },
		// Users: []*server.User{
		// 	{
		// 		Username: "admin",
		// 		Password: "admin",
		// 		Account:  &server.Account{Name: "$SYS"},
		// 	},
		// },
	}
	virtual, err := server.NewServer(virtualOpts)
	if err != nil {
		t.Fatal(err)
	}
	virtual.ConfigureLogger()

	if err := server.Run(virtual); err != nil {
		t.Fatal(err)
	}

	if !primero.ReadyForConnections(10 * time.Second) {
		t.Fatal("primero not ready")
	}

	if !virtual.ReadyForConnections(10 * time.Second) {
		t.Fatal("primero not ready")
	}

	if !primero.JetStreamIsClustered() {
		t.Fatal("primero is not clustered")
	}

	// How can I check if the servers are _really_ ready?
	time.Sleep(8 * time.Second)

	nc, err := nats.Connect(primero.ClientURL())
	if err != nil {
		t.Fatal(err)
	}

	jsp, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	_, err = jsp.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
		Bucket:   "testing",
		Replicas: 1,
		Placement: &jetstream.Placement{
			Tags: []string{"app:cascade"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	segundoOpts := &server.Options{
		ServerName: "segundo",
		Port:       4223,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Routes: []*url.URL{
			{Host: "localhost:6222"},
		},
		Tags: jwt.TagList{"app:cascade"},
		Cluster: server.ClusterOpts{
			Name: "cascade",
			Host: "localhost",
			Port: 6223,
		},
	}

	segundo, err := server.NewServer(segundoOpts)
	if err != nil {
		t.Fatal(err)
	}
	segundo.ConfigureLogger()

	if err := server.Run(segundo); err != nil {
		t.Fatal(err)
	}

	time.Sleep(8 * time.Second)
	if err := virtual.DisableJetStream(); err != nil {
		t.Fatal(err)
	}

	primeroOpts.Routes = []*url.URL{{Host: "localhost:6223"}}
	segundoOpts.Routes = []*url.URL{{Host: "localhost:6222"}}

	if err := primero.ReloadOptions(primeroOpts); err != nil {
		t.Fatal(err)
	}
	if err := segundo.ReloadOptions(segundoOpts); err != nil {
		t.Fatal(err)
	}

	virtual.Shutdown()
	virtual.WaitForShutdown()

	_, err = jsp.UpdateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
		Bucket:   "testing",
		Replicas: 3,
		Placement: &jetstream.Placement{
			Tags: []string{"app:cascade"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	terceroOpts := &server.Options{
		ServerName: "tercero",
		Port:       4224,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Routes: []*url.URL{
			{Host: "localhost:6222"},
			{Host: "localhost:6223"},
		},
		Tags: jwt.TagList{"app:cascade"},
		Cluster: server.ClusterOpts{
			Name: "cascade",
			Host: "localhost",
			Port: 6224,
		},
	}

	tercero, err := server.NewServer(terceroOpts)
	if err != nil {
		t.Fatal(err)
	}
	tercero.ConfigureLogger()

	if err := server.Run(tercero); err != nil {
		t.Fatal(err)
	}

	time.Sleep(8 * time.Second)

	primeroOpts.Routes = []*url.URL{{Host: "localhost:6223"}, {Host: "localhost:6224"}}
	segundoOpts.Routes = []*url.URL{{Host: "localhost:6222"}, {Host: "localhost:6224"}}

	if err := primero.ReloadOptions(primeroOpts); err != nil {
		t.Fatal(err)
	}
	if err := segundo.ReloadOptions(segundoOpts); err != nil {
		t.Fatal(err)
	}

	_, err = jsp.UpdateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
		Bucket:   "testing",
		Replicas: 3,
		Placement: &jetstream.Placement{
			Tags: []string{"app:cascade"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Second)
}
