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
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/robinkb/cascade/controller/core"
	"golang.org/x/exp/maps"
)

func NewServiceDiscovery(store *ServiceDiscoveryStore, clusterName string) core.ServiceDiscovery {
	return &serviceDiscovery{
		store:       store,
		clusterName: clusterName,
		refresh:     make(chan struct{}),
	}
}

type serviceDiscovery struct {
	store        *ServiceDiscoveryStore
	clusterName  string
	clusterRoute *core.ClusterRoute

	refresh chan struct{}
}

func (sd *serviceDiscovery) Start(stopCh <-chan struct{}) {
	go func() {
		for {
			select {
			case <-sd.store.Refresh(sd.clusterName):
				sd.sendRefresh()
			case <-stopCh:
				return
			}
		}
	}()
}

func (sd *serviceDiscovery) Register(clusterRoute *core.ClusterRoute) {
	sd.store.Set(sd.clusterName, clusterRoute.ServerName, &url.URL{
		Host: fmt.Sprintf("%s:%d", clusterRoute.IPAddr, clusterRoute.Port),
	})

	sd.clusterRoute = clusterRoute
	sd.sendRefresh()
}

func (sd *serviceDiscovery) Routes() ([]*url.URL, error) {
	return sd.store.Routes(sd.clusterName), nil
}

func (sd *serviceDiscovery) Refresh() <-chan struct{} {
	return sd.refresh
}

func (sd *serviceDiscovery) sendRefresh() {
	select {
	case sd.refresh <- struct{}{}:
	case <-time.After(1 * time.Millisecond):
	}
}

func NewServiceDiscoveryStore() *ServiceDiscoveryStore {
	return &ServiceDiscoveryStore{
		routes:  make(map[string]map[string]*url.URL),
		refresh: map[string]chan struct{}{},
	}
}

type ServiceDiscoveryStore struct {
	routes  map[string]map[string]*url.URL
	mu      sync.Mutex
	refresh map[string]chan struct{}
}

func (s *ServiceDiscoveryStore) Set(cluster, server string, u *url.URL) {
	s.mu.Lock()
	if s.routes[cluster] == nil {
		s.routes[cluster] = make(map[string]*url.URL)
	}
	if s.refresh[cluster] == nil {
		s.refresh[cluster] = make(chan struct{})
	}
	s.routes[cluster][server] = u
	s.mu.Unlock()

	s.sendRefresh(cluster)
}

func (s *ServiceDiscoveryStore) Routes(cluster string) []*url.URL {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Values(s.routes[cluster])
}

func (s *ServiceDiscoveryStore) Refresh(cluster string) <-chan struct{} {
	return s.refresh[cluster]
}

func (s *ServiceDiscoveryStore) sendRefresh(cluster string) {
	select {
	case s.refresh[cluster] <- struct{}{}:
	case <-time.After(1 * time.Millisecond):
	}
}
