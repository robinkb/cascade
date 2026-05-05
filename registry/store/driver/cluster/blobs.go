package cluster

import (
	"bytes"
	"io"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/registry/store"
)

const (
	tPutBlob      cluster.ProposalType = iota
	tDeleteBlob   cluster.ProposalType = iota
	tInitUpload   cluster.ProposalType = iota
	tAppendUpload cluster.ProposalType = iota
	tCloseUpload  cluster.ProposalType = iota
	tDeleteUpload cluster.ProposalType = iota
)

func NewBlobStore(proposer cluster.Proposer, blobs store.Blobs) store.Blobs {
	s := &blobStore{
		Blobs:    blobs,
		proposer: proposer,
	}

	proposer.Handle(tPutBlob, s.putBlob)
	proposer.Handle(tDeleteBlob, s.deleteBlob)
	proposer.Handle(tInitUpload, s.initUpload)
	proposer.Handle(tAppendUpload, s.appendUpload)
	proposer.Handle(tCloseUpload, s.closeUpload)
	proposer.Handle(tDeleteUpload, s.deleteUpload)

	return s
}

type blobStore struct {
	store.Blobs
	proposer cluster.Proposer
}

type pPutBlob struct {
	ID      digest.Digest
	Content []byte
}

func (s *blobStore) PutBlob(id digest.Digest, content []byte) error {
	p := &pPutBlob{
		ID:      id,
		Content: content,
	}
	_, err := s.proposer.Propose(tPutBlob, mustMarshal(p))
	return err
}

func (s *blobStore) putBlob(data []byte) (resp any, err error) {
	v := new(pPutBlob)
	mustUnmarshal(data, v)
	err = s.Blobs.PutBlob(v.ID, v.Content)
	return
}

type pDeleteBlob struct {
	ID digest.Digest
}

func (s *blobStore) DeleteBlob(id digest.Digest) error {
	p := &pDeleteBlob{
		ID: id,
	}
	_, err := s.proposer.Propose(tDeleteBlob, mustMarshal(p))
	return err
}

func (s *blobStore) deleteBlob(data []byte) (resp any, err error) {
	v := new(pDeleteBlob)
	mustUnmarshal(data, v)
	err = s.Blobs.DeleteBlob(v.ID)
	return
}

type pInitUpload struct {
	SessionID uuid.UUID
}

func (s *blobStore) InitUpload(id uuid.UUID) error {
	p := &pInitUpload{
		SessionID: id,
	}
	_, err := s.proposer.Propose(tInitUpload, mustMarshal(p))
	return err
}

func (s *blobStore) initUpload(data []byte) (resp any, err error) {
	v := new(pInitUpload)
	mustUnmarshal(data, v)
	err = s.Blobs.InitUpload(v.SessionID)
	return
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
	SessionID uuid.UUID
	Chunk     []byte
}

func (w *writer) flush() (n int, err error) {
	defer w.buf.Reset()
	p := &pAppendUpload{
		SessionID: w.sessionId,
		Chunk:     w.buf.Bytes(),
	}
	resp, err := w.proposer.Propose(tAppendUpload, mustMarshal(p))
	n = int(resp.(int64))
	return
}

func (w *writer) Close() error {
	_, err := w.flush()
	return err
}

func (s *blobStore) appendUpload(data []byte) (resp any, err error) {
	v := new(pAppendUpload)
	mustUnmarshal(data, v)
	w, err := s.Blobs.UploadWriter(v.SessionID)
	if err != nil {
		return
	}

	buf := bytes.NewBuffer(v.Chunk)
	n, err := io.Copy(w, buf)
	resp = n
	return
}

type pCloseUpload struct {
	SessionID uuid.UUID
	Digest    digest.Digest
}

func (s *blobStore) CloseUpload(id uuid.UUID, digest digest.Digest) error {
	p := &pCloseUpload{
		SessionID: id,
		Digest:    digest,
	}
	_, err := s.proposer.Propose(tCloseUpload, mustMarshal(p))
	return err
}

func (s *blobStore) closeUpload(data []byte) (resp any, err error) {
	v := new(pCloseUpload)
	mustUnmarshal(data, v)
	err = s.Blobs.CloseUpload(v.SessionID, v.Digest)
	return
}

type pDeleteUpload struct {
	SessionID uuid.UUID
}

func (s *blobStore) DeleteUpload(id uuid.UUID) error {
	p := &pDeleteUpload{
		SessionID: id,
	}
	_, err := s.proposer.Propose(tDeleteBlob, mustMarshal(p))
	return err
}

func (s *blobStore) deleteUpload(data []byte) (resp any, err error) {
	v := new(pDeleteUpload)
	mustUnmarshal(data, v)
	err = s.Blobs.DeleteUpload(v.SessionID)
	return
}
