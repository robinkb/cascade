package cluster

import (
	"math/rand/v2"

	"github.com/gofrs/uuid/v5"
	"github.com/robinkb/cascade-registry/cluster"
	"github.com/robinkb/cascade-registry/store"
)

func NewBlobStore(node cluster.Node, blobs store.Blobs) store.Blobs {
	s := &blobStore{
		Blobs: blobs,
		node:  node,
	}

	node.Process(&initUpload{}, s.initUpload)

	return &blobStore{
		Blobs: blobs,
		node:  node,
	}
}

type blobStore struct {
	store.Blobs
	node cluster.Node
}

func (s *blobStore) InitUpload(id uuid.UUID) error {
	op := &initUpload{
		rand.Uint64(),
		id,
	}
	return s.node.Propose(op)
}

func (s *blobStore) initUpload(op cluster.Operation) error {
	v := op.(*initUpload)
	return s.Blobs.InitUpload(v.SessionID)
}
