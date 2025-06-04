package raft

import (
	"encoding/gob"

	"github.com/opencontainers/go-digest"
)

func init() {
	// Implementers of the operation interface must be registered
	// with gob so that it can encode then as an operation type,
	// and decode them back to the concrete type.
	gob.Register(&putTag{})
}

// operation represents an operation that gets commited to the Raft log.
type operation interface {
	ID() uint64
}

type putTag struct {
	Id     uint64
	Name   string
	Tag    string
	Digest digest.Digest
}

func (t *putTag) ID() uint64 {
	return t.Id
}
