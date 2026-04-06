package cluster

import (
	"bytes"
	"io"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/registry/store"
)

func NewBlobStore(proposer cluster.Proposer, blobs store.Blobs) store.Blobs {
	s := &blobStore{
		Blobs:    blobs,
		proposer: proposer,
	}

	proposer.Handle(&pPutBlob{}, s.putBlob)
	proposer.Handle(&pDeleteBlob{}, s.deleteBlob)
	proposer.Handle(&pInitUpload{}, s.initUpload)
	proposer.Handle(&pAppendUpload{}, s.appendUpload)
	proposer.Handle(&pCloseUpload{}, s.closeUpload)
	proposer.Handle(&pDeleteUpload{}, s.deleteUpload)

	return s
}

type blobStore struct {
	store.Blobs
	proposer cluster.Proposer
}

type pPutBlob struct {
	cluster.ProposalBase
	ID      digest.Digest
	Content []byte
}

func (s *blobStore) PutBlob(id digest.Digest, content []byte) error {
	p := &pPutBlob{
		ID:      id,
		Content: content,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) putBlob(p cluster.Proposal) cluster.Response {
	v := p.(*pPutBlob)
	return s.Blobs.PutBlob(v.ID, v.Content)
}

type pDeleteBlob struct {
	cluster.ProposalBase
	ID digest.Digest
}

func (s *blobStore) DeleteBlob(id digest.Digest) error {
	p := &pDeleteBlob{
		ID: id,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) deleteBlob(p cluster.Proposal) cluster.Response {
	v := p.(*pDeleteBlob)
	return s.Blobs.DeleteBlob(v.ID)
}

type pInitUpload struct {
	cluster.ProposalBase
	SessionID uuid.UUID
}

func (s *blobStore) InitUpload(id uuid.UUID) error {
	p := &pInitUpload{
		SessionID: id,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) initUpload(p cluster.Proposal) cluster.Response {
	v := p.(*pInitUpload)
	return s.Blobs.InitUpload(v.SessionID)
}

func (s *blobStore) UploadWriter(id uuid.UUID) (io.WriteCloser, error) {
	return newWriter(s.proposer, id), nil
}

const (
	// TODO: Make this configurable?
	writeBufferSize = 1 << 20
)

func newWriter(proposer cluster.Proposer, sessionId uuid.UUID) *writer {
	w := &writer{
		proposer:  proposer,
		sessionId: sessionId,
		buf:       new(bytes.Buffer),
	}

	return w
}

type writer struct {
	proposer  cluster.Proposer
	sessionId uuid.UUID
	buf       *bytes.Buffer
}

func (w *writer) Write(p []byte) (n int, err error) {
	if w.buf.Len()+len(p) > writeBufferSize {
		n, err = w.flush()
		if err != nil {
			return n, err
		}
	}

	return w.buf.Write(p)
}

type pAppendUpload struct {
	cluster.ProposalBase
	SessionID uuid.UUID
	Chunk     []byte
}

type rAppendUpload struct {
	cluster.ResponseBase
	N int
}

func (w *writer) flush() (n int, err error) {
	defer w.buf.Reset()
	p := &pAppendUpload{
		SessionID: w.sessionId,
		Chunk:     w.buf.Bytes(),
	}
	r := w.proposer.Propose(p).(*rAppendUpload)
	return r.N, r.Err
}

func (w *writer) Close() error {
	_, err := w.flush()
	return err
}

func (s *blobStore) appendUpload(p cluster.Proposal) cluster.Response {
	v := p.(*pAppendUpload)
	w, err := s.Blobs.UploadWriter(v.SessionID)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(v.Chunk)
	n, err := io.Copy(w, buf)
	r := new(rAppendUpload)
	r.N = int(n)
	r.Err = err
	return r
}

type pCloseUpload struct {
	cluster.ProposalBase
	SessionID uuid.UUID
	Digest    digest.Digest
}

func (s *blobStore) CloseUpload(id uuid.UUID, digest digest.Digest) error {
	p := &pCloseUpload{
		SessionID: id,
		Digest:    digest,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) closeUpload(p cluster.Proposal) cluster.Response {
	v := p.(*pCloseUpload)
	return s.Blobs.CloseUpload(v.SessionID, v.Digest)
}

type pDeleteUpload struct {
	cluster.ProposalBase
	SessionID uuid.UUID
}

func (s *blobStore) DeleteUpload(id uuid.UUID) error {
	p := &pDeleteUpload{
		SessionID: id,
	}
	return s.proposer.Propose(p)
}

func (s *blobStore) deleteUpload(p cluster.Proposal) cluster.Response {
	v := p.(*pDeleteUpload)
	return s.Blobs.DeleteUpload(v.SessionID)
}
