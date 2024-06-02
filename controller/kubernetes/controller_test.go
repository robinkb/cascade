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
package kubernetes

import (
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/robinkb/cascade/controller/core"
	"github.com/robinkb/cascade/controller/core/testsuites"
)

func TestKubernetesController(t *testing.T) {
	client := createTestingClient(t)
	namespace := createTestingNamespace(t, client)

	controllerConstructor := func(clusterName string, clusterRoute *core.ClusterRoute) (core.Controller, error) {
		return NewController(client, namespace, clusterRoute, &server.Options{
			JetStream:  false,
			StoreDir:   t.TempDir(),
			Port:       -1,
			ServerName: clusterRoute.ServerName,
			Cluster: server.ClusterOpts{
				Name: clusterName,
				Port: -1,
			},
			DisableJetStreamBanner: true,
		})
	}

	testsuites.Controller(t, controllerConstructor)
}
