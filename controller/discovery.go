package controller

import (
	"net/url"
	"sync"

	"golang.org/x/exp/maps"
)

func NewDiscoveryClient() *discoveryClient {
	return &discoveryClient{
		endpoints: make(map[string]*url.URL),
	}
}

type discoveryClient struct {
	mu        sync.Mutex
	endpoints map[string]*url.URL
}

func (dc *discoveryClient) Registered(id string) bool {
	_, ok := dc.endpoints[id]
	return ok
}

func (dc *discoveryClient) SetEndpoint(id string, url *url.URL) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.endpoints[id] = url
}

func (dc *discoveryClient) Endpoints() []*url.URL {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return maps.Values(dc.endpoints)
}
