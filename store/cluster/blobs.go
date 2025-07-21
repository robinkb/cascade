package cluster

import (
	"bytes"
	"io"
	"math/rand/v2"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"

	"github.com/robinkb/cascade-registry/cluster/raft"
	"github.com/robinkb/cascade-registry/store"
)

func NewBlobStore(proposer raft.Proposer, blobs store.Blobs) store.Blobs {
	s := &blobStore{
		Blobs:    blobs,
		proposer: proposer,
	}

	proposer.Handle(&putBlob{}, s.putBlob)
	proposer.Handle(&deleteBlob{}, s.deleteBlob)
	proposer.Handle(&initUpload{}, s.initUpload)
	proposer.Handle(&appendUpload{}, s.appendUpload)
	proposer.Handle(&closeUpload{}, s.closeUpload)
	proposer.Handle(&deleteUpload{}, s.deleteUpload)

	return s
}

type blobStore struct {
	store.Blobs
	proposer raft.Proposer
}

func (s *blobStore) PutBlob(id digest.Digest, content []byte) error {
	p := &putBlob{
		rand.Uint64(),
		id, content,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) putBlob(p raft.Proposal) error {
	v := p.(*putBlob)
	return s.Blobs.PutBlob(v.Digest, v.Content)
}

func (s *blobStore) DeleteBlob(id digest.Digest) error {
	p := &deleteBlob{
		rand.Uint64(),
		id,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) deleteBlob(p raft.Proposal) error {
	v := p.(*deleteBlob)
	return s.Blobs.DeleteBlob(v.Digest)
}

func (s *blobStore) InitUpload(id uuid.UUID) error {
	p := &initUpload{
		rand.Uint64(),
		id,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) initUpload(p raft.Proposal) error {
	v := p.(*initUpload)
	return s.Blobs.InitUpload(v.SessionID)
}

func (s *blobStore) UploadWriter(id uuid.UUID) (io.WriteCloser, error) {
	return &writer{
		buf:       new(bytes.Buffer),
		proposer:  s.proposer,
		sessionId: id,
	}, nil
}

func (s *blobStore) appendUpload(p raft.Proposal) error {
	v := p.(*appendUpload)
	w, err := s.Blobs.UploadWriter(v.SessionID)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(v.Data)
	_, err = io.Copy(w, buf)
	return err
}

func (s *blobStore) CloseUpload(id uuid.UUID, digest digest.Digest) error {
	p := &closeUpload{
		rand.Uint64(),
		id, digest,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) closeUpload(p raft.Proposal) error {
	v := p.(*closeUpload)
	return s.Blobs.CloseUpload(v.SessionID, v.Digest)
}

func (s *blobStore) DeleteUpload(id uuid.UUID) error {
	p := &deleteUpload{
		rand.Uint64(),
		id,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) deleteUpload(p raft.Proposal) error {
	v := p.(*deleteUpload)
	return s.Blobs.DeleteUpload(v.SessionID)
}

const (
	// TODO: Make this configurable?
	writeBufferSize = 1 << 20
)

type writer struct {
	proposer  raft.Proposer
	sessionId uuid.UUID
	buf       *bytes.Buffer
}

func (w *writer) Write(p []byte) (n int, err error) {
	if w.buf.Len()+len(p) > writeBufferSize {
		err = w.flush()
		if err != nil {
			return 0, err
		}
	}

	return w.buf.Write(p)
}

func (w *writer) flush() error {
	defer w.buf.Reset()
	return w.proposer.Propose(&appendUpload{
		rand.Uint64(),
		w.sessionId,
		w.buf.Bytes(),
	})
}

func (w *writer) Close() error {
	return w.flush()
}
