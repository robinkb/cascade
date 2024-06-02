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
	"github.com/nats-io/nats-server/v2/server"
	"github.com/robinkb/cascade/controller/core"
	"github.com/robinkb/cascade/controller/core/nats"
	"k8s.io/client-go/kubernetes"
)

func NewController(client kubernetes.Interface, namespace string, clusterRoute *core.ClusterRoute, options *server.Options) (core.Controller, error) {
	sd, err := NewServiceDiscovery(client, namespace, options.Cluster.Name)
	if err != nil {
		return nil, err
	}

	return core.NewController(
		sd,
		nats.NewServer(options),
	), nil
}