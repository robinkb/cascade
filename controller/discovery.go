package controller

import (
	"net/url"
	"sync"

	"golang.org/x/exp/maps"
)

func NewDiscoveryClient() *discoveryClient {
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

func (dc *discoveryClient) SetEndpoint(id string, url *url.URL) {
	dc.mu.Lock()
	dc.endpoints[id] = url
	dc.mu.Unlock()

	// Send refreshes for every registered endpoint.
	// Does not account for endpoints going away atm,
	// but it's good enough for now.
	for range len(dc.endpoints) {
		dc.refresh <- struct{}{}
	}
}

func (dc *discoveryClient) Refresh() <-chan struct{} {
	return dc.refresh
}

func (dc *discoveryClient) Endpoints() []*url.URL {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return maps.Values(dc.endpoints)
}
