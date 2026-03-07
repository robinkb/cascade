package cluster

import "errors"

var (
	ErrDuplicateClient = errors.New("duplicate client")
	ErrClientNotFound  = errors.New("client not found")
)

func NewClients[T any]() Clients[T] {
	return Clients[T]{
		clients: make(map[uint64]*T),
	}
}

type Clients[T any] struct {
	clients map[uint64]*T
}

func (c *Clients[T]) Add(id uint64, client *T) error {
	if _, ok := c.clients[id]; ok {
		return ErrDuplicateClient
	}
	c.clients[id] = client
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
}
