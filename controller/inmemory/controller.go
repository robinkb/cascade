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
	"github.com/nats-io/nats-server/v2/server"
	"github.com/robinkb/cascade/controller/core"
	"github.com/robinkb/cascade/controller/core/nats"
)

var defaultServiceDiscoveryStore = NewServiceDiscoveryStore()

func NewController(clusterRoute *core.ClusterRoute, options *server.Options) (core.Controller, error) {
	ns, err := nats.NewServer(options)
	if err != nil {
		return nil, err
	}

	return core.NewController(
		NewServiceDiscovery(defaultServiceDiscoveryStore, options.Cluster.Name),
		ns,
	), nil
}
