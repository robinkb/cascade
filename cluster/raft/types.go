package raft

import (
	"encoding/gob"

	"github.com/opencontainers/go-digest"
)

func init() {
	// Implementers of the Message interface must be registered
	// with gob so that it can encode and decode them.
	gob.Register(&PutTag{})
}

type (
	Message interface {
		ID() uint64
	}

	Operation struct {
		ID uint64
	}

	PutTag struct {
		Operation
		Name, Tag string
		Digest    digest.Digest
	}
)

func (t *PutTag) ID() uint64 {
	return t.Operation.ID
}
