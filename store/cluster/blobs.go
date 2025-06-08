package cluster

import (
	"bytes"
	"io"
	"math/rand/v2"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/cluster"
	"github.com/robinkb/cascade-registry/store"
)

func NewBlobStore(node cluster.Node, blobs store.Blobs) store.Blobs {
	s := &blobStore{
		Blobs: blobs,
		node:  node,
	}

	node.Handle(&putBlob{}, s.putBlob)
	node.Handle(&deleteBlob{}, s.deleteBlob)
	node.Handle(&initUpload{}, s.initUpload)
	node.Handle(&appendUpload{}, s.appendUpload)
	node.Handle(&closeUpload{}, s.closeUpload)
	node.Handle(&deleteUpload{}, s.deleteUpload)

	return s
}

type blobStore struct {
	store.Blobs
	node cluster.Node
}

func (s *blobStore) PutBlob(id digest.Digest, content []byte) error {
	op := &putBlob{
		rand.Uint64(),
		id, content,
	}
	return s.node.Propose(op)
}

func (s *blobStore) putBlob(op cluster.Operation) error {
	v := op.(*putBlob)
	return s.Blobs.PutBlob(v.Digest, v.Content)
}

func (s *blobStore) DeleteBlob(id digest.Digest) error {
	op := &deleteBlob{
		rand.Uint64(),
		id,
	}
	return s.node.Propose(op)
}

func (s *blobStore) deleteBlob(op cluster.Operation) error {
	v := op.(*deleteBlob)
	return s.Blobs.DeleteBlob(v.Digest)
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

func (s *blobStore) UploadWriter(id uuid.UUID) (io.Writer, error) {
	return &writer{
		node:      s.node,
		sessionId: id,
	}, nil
}

func (s *blobStore) appendUpload(op cluster.Operation) error {
	v := op.(*appendUpload)
	w, err := s.Blobs.UploadWriter(v.SessionID)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(v.Data)
	_, err = io.Copy(w, buf)
	return err
}

func (s *blobStore) CloseUpload(id uuid.UUID, digest digest.Digest) error {
	op := &closeUpload{
		rand.Uint64(),
		id, digest,
	}
	return s.node.Propose(op)
}

func (s *blobStore) closeUpload(op cluster.Operation) error {
	v := op.(*closeUpload)
	return s.Blobs.CloseUpload(v.SessionID, v.Digest)
}

func (s *blobStore) DeleteUpload(id uuid.UUID) error {
	op := &deleteUpload{
		rand.Uint64(),
		id,
	}
	return s.node.Propose(op)
}

func (s *blobStore) deleteUpload(op cluster.Operation) error {
	v := op.(*deleteUpload)
	return s.Blobs.DeleteUpload(v.SessionID)
}

type writer struct {
	node      cluster.Node
	sessionId uuid.UUID
}

func (w *writer) Write(p []byte) (n int, err error) {
	op := &appendUpload{
		rand.Uint64(),
		w.sessionId, p,
	}
	err = w.node.Propose(op)
	n = len(p)
	return
}
