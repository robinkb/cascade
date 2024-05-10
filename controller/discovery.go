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

func (c *discoveryClient) AddEndpoint(id string, url *url.URL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.endpoints[id] = url
}

func (c *discoveryClient) Endpoints() []*url.URL {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Values(c.endpoints)
}
