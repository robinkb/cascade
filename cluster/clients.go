package cluster

import (
	"errors"
	"maps"
	"slices"
)

var (
	ErrDuplicateClient = errors.New("duplicate client")
	ErrClientNotFound  = errors.New("client not found")
)

func NewClients[T any]() Clients[T] {
	return Clients[T]{
		clients: make(map[uint64]*T),
		peers:   make(map[uint64]Peer),
	}
}

type Clients[T any] struct {
	clients map[uint64]*T
	peers   map[uint64]Peer
}

func (c *Clients[T]) Add(peer Peer, client *T) error {
	if _, ok := c.clients[peer.ID]; ok {
		return ErrDuplicateClient
	}
	c.clients[peer.ID] = client
	c.peers[peer.ID] = peer
	return nil
}

func (c *Clients[T]) Get(id uint64) (*T, error) {
	if client, ok := c.clients[id]; ok {
		return client, nil
	}
	return nil, ErrClientNotFound
}

func (c *Clients[T]) Remove(id uint64) {
	delete(c.clients, id)
	delete(c.peers, id)
}

func (c *Clients[T]) Peers() []Peer {
	return slices.Collect(maps.Values(c.peers))
}
