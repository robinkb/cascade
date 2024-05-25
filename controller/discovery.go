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
package controller

import (
	"net/url"
	"sync"
	"time"

	"golang.org/x/exp/maps"
)

func NewInMemoryDiscoveryClient() *discoveryClient {
	return &discoveryClient{
		endpoints: make(map[string]*url.URL),
		refresh:   make(chan struct{}, 1),
	}
}

type discoveryClient struct {
	mu        sync.Mutex
	endpoints map[string]*url.URL
	refresh   chan struct{}
}

func (dc *discoveryClient) Registered(id string) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	_, ok := dc.endpoints[id]
	return ok
}

func (dc *discoveryClient) Set(id string, url *url.URL) {
	dc.mu.Lock()
	dc.endpoints[id] = url
	dc.mu.Unlock()

	dc.sendRefreshes()
}

func (dc *discoveryClient) Delete(id string) {
	dc.mu.Lock()
	delete(dc.endpoints, id)
	dc.mu.Unlock()

	dc.sendRefreshes()
}

func (dc *discoveryClient) Refresh() <-chan struct{} {
	return dc.refresh
}

func (dc *discoveryClient) sendRefreshes() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	for range len(dc.endpoints) - 1 {
		select {
		case dc.refresh <- struct{}{}:
		case <-time.After(1 * time.Second):
		}
	}
}

func (dc *discoveryClient) Routes() []*url.URL {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return maps.Values(dc.endpoints)
}
