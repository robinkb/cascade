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
package nats

import (
	"testing"
	"time"

	nats "github.com/nats-io/nats-server/v2/server"
)

func TestServer(t *testing.T) {
	s1opts := makeOptions(t, "s1")
	s1 := NewServer(s1opts)
	if err := s1.Start(); err != nil {
		t.Fatal(err)
	}

	s2opts := makeOptions(t, "s2")
	s2 := NewServer(s2opts)
	if err := s2.Start(); err != nil {
		t.Fatal(err)
	}

	s1.SetRoute("s2", s2.ClusterRoute())
	s2.SetRoute("s1", s1.ClusterRoute())

	s1.server.ReadyForConnections(4 * time.Second)
	s2.server.ReadyForConnections(4 * time.Second)

	if s1.ActivePeers() != 2 || s2.ActivePeers() != 2 {
		t.Fatal("not clustered properly")
	}

	s3opts := makeOptions(t, "s3")
	s3 := NewServer(s3opts)
	if err := s3.Start(); err != nil {
		t.Fatal(err)
	}

	s1.SetRoute("s3", s3.ClusterRoute())
	s2.SetRoute("s3", s3.ClusterRoute())
	s3.SetRoute("s1", s1.ClusterRoute())
	s3.SetRoute("s2", s2.ClusterRoute())

	time.Sleep(2 * time.Second)
	t.Log(s1.ActivePeers())
	t.Log(s2.ActivePeers())
	t.Log(s3.ActivePeers())
	time.Sleep(10 * time.Second)
	t.Log(s1.server.JetStreamClusterPeers())
	t.Log(s2.server.JetStreamClusterPeers())
	t.Log(s3.server.JetStreamClusterPeers())
}

func makeOptions(t *testing.T, name string) *nats.Options {
	t.Helper()

	return &nats.Options{
		ServerName: name,
		Port:       -1,
		JetStream:  false,
		StoreDir:   t.TempDir(),
		Cluster: nats.ClusterOpts{
			Name: "testing",
			Host: "localhost",
			Port: -1,
		},
		DisableJetStreamBanner: true,
	}
}

func dumpRoutes(t *testing.T, server *server) {
	t.Helper()
	routez, err := server.server.Routez(&nats.RoutezOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("id: %s, name: %s, numRoutes: %d", routez.ID, routez.Name, routez.NumRoutes)
	for _, route := range routez.Routes {
		t.Logf("remoteID: %s, remoteName: %s, host: %s:%d", route.RemoteID, route.RemoteName, route.IP, route.Port)
	}
}
