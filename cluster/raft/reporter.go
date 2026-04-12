package raft

import (
	"math/rand/v2"
	"sync"
)

// NewReporter returns a new Reporter for type T.
func NewReporter[T any]() Reporter[T] {
	return Reporter[T]{
		channels: make(map[uint64]chan T),
	}
}

// Reporter manages channels of type T identified by an ID.
type Reporter[T any] struct {
	mu       sync.Mutex
	channels map[uint64]chan T
}

// Create returns a new ID and receiving channel of type T.
// It is used to await a result from another go routine
// by calling [Reporter.Get] with the received ID.
func (r *Reporter[T]) Create() (uint64, <-chan T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.createId()
	r.channels[id] = make(chan T)
	return id, r.channels[id]
}

func (r *Reporter[T]) createId() uint64 {
	for {
		// Assuming that there are 10000 active channels,
		// there is a 0.000000000271% chance of a duplicate.
		// That's not 0.
		id := rand.Uint64()
		if r.channels[id] == nil {
			return id
		}
	}
}

// Get returns a sending channel identified by the id,
// if it was previously created. If a channel for the id
// cannot be found, the boolean return value is false.
// It is used to send a result to the go routine that
// created the channel using [Reporter.Create].
func (r *Reporter[T]) Get(id uint64) (chan<- T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	resultC, ok := r.channels[id]
	return resultC, ok
}

// Delete closes and deletes the channel.
func (r *Reporter[T]) Delete(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	close(r.channels[id])
	delete(r.channels, id)
}
