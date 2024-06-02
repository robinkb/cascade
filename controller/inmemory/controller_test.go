/*
Copyright © 2024 Robin Ketelbuters

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package inmemory

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/robinkb/cascade/controller/core"
)

// I swear that this is the easiest way to do it.
const registryConf = `
version: 0.1
storage:
  nats: {}
`

func TestClusterFormation(t *testing.T) {
	t.Skip("TODO: refactor into a testsuite")

	controllers := make([]core.Controller, 0)

	// Initialize the controllers
	for i := 0; i < 3; i++ {
		clusterRoute := core.ClusterRoute{
			ServerName: fmt.Sprintf("s%d", i),
			IPAddr:     "localhost",
			Port:       6222 + int32(i),
		}

		controllers = append(controllers, NewController(clusterRoute, &server.Options{
			JetStream:  false,
			StoreDir:   t.TempDir(),
			Port:       -1,
			ServerName: clusterRoute.ServerName,
			Cluster: server.ClusterOpts{
				Name: "cascade",
				Host: clusterRoute.IPAddr,
				Port: int(clusterRoute.Port),
			},
		}))
	}

	// Start all of them
	for _, c := range controllers {
		c.Start()
	}

	time.Sleep(10 * time.Second)

	// // Wait for all NATS servers to have started.
	// // Maybe this should be a StatusNATS call on the core.
	// for _, c := range controllers {
	// 	for {
	// 		if c.ns == nil {
	// 			time.Sleep(100 * time.Millisecond)
	// 			continue
	// 		}

	// 		if !c.ns.ReadyForConnections(4 * time.Second) {
	// 			continue
	// 		}

	// 		break
	// 	}
	// }

	// // Check if all of them are clustered. Not sure if this is a good check.
	// for _, c := range controllers {
	// 	if !c.ns.JetStreamIsClustered() {
	// 		t.Error("not clustered")
	// 	}
	// }

	// // Shut it all down.
	// for _, c := range controllers {
	// 	c.Shutdown()
	// 	c.WaitForShutdown()
	// }

	t.Log("shutdown complete")
}

// 1. Start virtual node with no tags
// 2. Start actual node tagged with "cascade" or something
// 3. Any created streams must select the "cascade" tag for placement
// 4. Second real node joins the cluster
// 5. Virtual node is removed

// This works!! 🎉
// TODO:
//   - Generate admin and registry users
//   - Cascade in its own NATS account
//   - mTLS certs
//   - Check how many peers the cluster has
func TestClusterBootstrap(t *testing.T) {
	t.Skip("proof skipped")

	routes := make([]*url.URL, 0)

	virtualOpts := makeNATSTestOptions(t, 0)
	virtualOpts.Tags = nil // Virtual server should be untagged.
	routes = append(routes, makeRouteURL(virtualOpts))

	server1Opts := makeNATSTestOptions(t, 1)
	routes = append(routes, makeRouteURL(server1Opts))

	virtualOpts.Routes = routes
	server1Opts.Routes = routes

	virtual, err := server.NewServer(virtualOpts.Clone())
	if err != nil {
		t.Fatal(err)
	}
	virtual.ConfigureLogger()
	virtual.Start()

	server1, err := server.NewServer(server1Opts.Clone())
	if err != nil {
		t.Fatal(err)
	}
	server1.ConfigureLogger()
	server1.Start()

	if !virtual.ReadyForConnections(10 * time.Second) {
		t.Fatal("server1 not ready")
	}

	if !server1.ReadyForConnections(10 * time.Second) {
		t.Fatal("server1 not ready")
	}

	if !server1.JetStreamIsClustered() {
		t.Fatal("server1 is not clustered")
	}

	// How can I check if the servers are _really_ ready?
	time.Sleep(8 * time.Second)

	nc1, err := nats.Connect(server1.ClientURL())
	if err != nil {
		t.Fatal(err)
	}

	js1, err := jetstream.New(nc1)
	if err != nil {
		t.Fatal(err)
	}

	// TODO: Also put some objects in here.
	_, err = js1.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
		Bucket:   "testing",
		Replicas: 1,
		Placement: &jetstream.Placement{
			Tags: []string{"app:cascade"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	server2Opts := makeNATSTestOptions(t, 2)
	routes = append(routes, makeRouteURL(server2Opts))
	server2Opts.Routes = routes

	server2, err := server.NewServer(server2Opts.Clone())
	if err != nil {
		t.Fatal(err)
	}
	server2.ConfigureLogger()
	server2.Start()

	time.Sleep(8 * time.Second)
	routes = routes[1:]
	if err := virtual.DisableJetStream(); err != nil {
		t.Fatal(err)
	}

	server1Opts.Routes = routes
	server2Opts.Routes = routes

	if err := server1.ReloadOptions(server1Opts.Clone()); err != nil {
		t.Fatal(err)
	}
	if err := server2.ReloadOptions(server2Opts.Clone()); err != nil {
		t.Fatal(err)
	}

	virtual.Shutdown()
	virtual.WaitForShutdown()

	_, err = js1.UpdateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
		Bucket:   "testing",
		Replicas: 2,
		Placement: &jetstream.Placement{
			Tags: []string{"app:cascade"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	server3Opts := makeNATSTestOptions(t, 3)
	routes = append(routes, makeRouteURL(server3Opts))
	server3Opts.Routes = routes

	server3, err := server.NewServer(server3Opts.Clone())
	if err != nil {
		t.Fatal(err)
	}
	server3.ConfigureLogger()

	if err := server.Run(server3); err != nil {
		t.Fatal(err)
	}

	server1Opts.Routes = routes
	server2Opts.Routes = routes

	if err := server1.ReloadOptions(server1Opts.Clone()); err != nil {
		t.Fatal(err)
	}
	if err := server2.ReloadOptions(server2Opts.Clone()); err != nil {
		t.Fatal(err)
	}

	time.Sleep(8 * time.Second)

	_, err = js1.UpdateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
		Bucket:   "testing",
		Replicas: 3,
		Placement: &jetstream.Placement{
			Tags: []string{"app:cascade"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Second)
}

// makeNATSTestOptions returns NATS Server options suitable for testing.
func makeNATSTestOptions(t *testing.T, index int) *server.Options {
	t.Helper()
	return &server.Options{
		ServerName: fmt.Sprintf("s%d", index),
		Port:       4222 + index,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		Tags:       jwt.TagList{"app:cascade"},
		Cluster: server.ClusterOpts{
			Name: "cascade",
			Host: "localhost",
			Port: 6222 + index,
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
}

func makeRouteURL(opts *server.Options) *url.URL {
	return &url.URL{
		Host: fmt.Sprintf(
			"%s:%d",
			opts.Cluster.Host,
			opts.Cluster.Port,
		),
	}
}
