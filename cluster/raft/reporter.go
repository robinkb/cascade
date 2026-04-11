package raft

import "sync"

// NewReporter returns a new Reporter for type T.
func NewReporter[T any]() Reporter[T] {
	return Reporter[T]{
		proposals: make(map[uint64]chan T),
	}
}

// Reporter manages channels of type T identified by an ID.
type Reporter[T any] struct {
	mu        sync.Mutex
	proposals map[uint64]chan T
}

// Create returns a new receiving channel of type T.
// It is used to await a result from another go routine.
func (r *Reporter[T]) Create(id uint64) <-chan T {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proposals[id] = make(chan T)
	return r.proposals[id]
}

// Get returns a sending channel identified by the id,
// if it was previously created. If a channel for the id
// cannot be found, the boolean return value is false.
// It is used to send a result to the go routine that
// created the channel.
func (r *Reporter[T]) Get(id uint64) (chan<- T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	resultC, ok := r.proposals[id]
	return resultC, ok
}

// Delete closes and deletes the channel.
func (r *Reporter[T]) Delete(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	close(r.proposals[id])
	delete(r.proposals, id)
}
