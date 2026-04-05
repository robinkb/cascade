package cluster

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/registry/store"
)

const (
	opPutBlob      = "B_PUT_BLOB"
	opDeleteBlob   = "B_DELETE_BLOB"
	opInitUpload   = "B_INIT_UPLOAD"
	opAppendUpload = "B_APPEND_UPLOAD"
	opCloseUpload  = "B_CLOSE_UPLOAD"
	opDeleteUpload = "B_DELETE_UPLOAD"
)

func NewBlobStore(proposer cluster.Proposer, blobs store.Blobs) store.Blobs {
	s := &blobStore{
		Blobs:    blobs,
		proposer: proposer,
	}

	proposer.Handle(opPutBlob, s.putBlob)
	proposer.Handle(opDeleteBlob, s.deleteBlob)
	proposer.Handle(opInitUpload, s.initUpload)
	proposer.Handle(opAppendUpload, s.appendUpload)
	proposer.Handle(opCloseUpload, s.closeUpload)
	proposer.Handle(opDeleteUpload, s.deleteUpload)

	return s
}

type blobStore struct {
	store.Blobs
	proposer cluster.Proposer
}

type putBlobRequest struct {
	ID      digest.Digest
	Content []byte
}

func (s *blobStore) PutBlob(id digest.Digest, content []byte) error {
	req := &putBlobRequest{
		ID:      id,
		Content: content,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := s.proposer.Propose(cluster.Request{
		Op:   opPutBlob,
		Data: data,
	})

	return resp.Err
}

func (s *blobStore) putBlob(req cluster.Request) (resp cluster.Response) {
	var putBlobRequest putBlobRequest
	err := json.Unmarshal(req.Data, &putBlobRequest)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = s.Blobs.PutBlob(putBlobRequest.ID, putBlobRequest.Content)
	return
}

type deleteBlobRequest struct {
	ID digest.Digest
}

func (s *blobStore) DeleteBlob(id digest.Digest) error {
	req := &deleteBlobRequest{
		ID: id,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := s.proposer.Propose(cluster.Request{
		Op:   opDeleteBlob,
		Data: data,
	})

	return resp.Err
}

func (s *blobStore) deleteBlob(req cluster.Request) (resp cluster.Response) {
	var deleteBlobRequest deleteBlobRequest
	err := json.Unmarshal(req.Data, &deleteBlobRequest)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = s.Blobs.DeleteBlob(deleteBlobRequest.ID)
	return
}

type initUploadRequest struct {
	SessionID uuid.UUID
}

func (s *blobStore) InitUpload(id uuid.UUID) error {
	req := initUploadRequest{
		SessionID: id,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := s.proposer.Propose(cluster.Request{
		Op:   opInitUpload,
		Data: data,
	})

	return resp.Err
}

func (s *blobStore) initUpload(req cluster.Request) (resp cluster.Response) {
	var initUploadRequest initUploadRequest
	err := json.Unmarshal(req.Data, &initUploadRequest)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = s.Blobs.InitUpload(initUploadRequest.SessionID)
	return
}

func (s *blobStore) UploadWriter(id uuid.UUID) (io.WriteCloser, error) {
	return &writer{
		buf:       new(bytes.Buffer),
		proposer:  s.proposer,
		sessionId: id,
	}, nil
}

const (
	// TODO: Make this configurable?
	writeBufferSize = 1 << 20
)

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

type appendUploadRequest struct {
	SessionID uuid.UUID
	Chunk     []byte
}

func (w *writer) flush() (n int, err error) {
	defer w.buf.Reset()

	req := &appendUploadRequest{
		SessionID: w.sessionId,
		Chunk:     w.buf.Bytes(),
	}
	data, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}

	resp := w.proposer.Propose(cluster.Request{
		Op:   opAppendUpload,
		Data: data,
	})

	n = int(binary.LittleEndian.Uint64(resp.Data))
	err = resp.Err
	return
}

func (w *writer) Close() error {
	_, err := w.flush()
	return err
}

func (s *blobStore) appendUpload(req cluster.Request) (resp cluster.Response) {
	var appendUploadRequest appendUploadRequest
	err := json.Unmarshal(req.Data, &appendUploadRequest)
	if err != nil {
		resp.Err = err
		return
	}

	w, err := s.Blobs.UploadWriter(appendUploadRequest.SessionID)
	if err != nil {
		resp.Err = err
		return
	}

	buf := bytes.NewBuffer(appendUploadRequest.Chunk)
	n, err := io.Copy(w, buf)
	resp.Data = make([]byte, 8)
	binary.LittleEndian.PutUint64(resp.Data, uint64(n))
	resp.Err = err
	return
}

type closeUploadRequest struct {
	SessionID uuid.UUID
	Digest    digest.Digest
}

func (s *blobStore) CloseUpload(id uuid.UUID, digest digest.Digest) error {
	req := &closeUploadRequest{
		SessionID: id,
		Digest:    digest,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := s.proposer.Propose(cluster.Request{
		Op:   opCloseUpload,
		Data: data,
	})

	return resp.Err
}

func (s *blobStore) closeUpload(req cluster.Request) (resp cluster.Response) {
	var closeUploadRequest closeUploadRequest
	err := json.Unmarshal(req.Data, &closeUploadRequest)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = s.Blobs.CloseUpload(closeUploadRequest.SessionID, closeUploadRequest.Digest)
	return
}

type deleteUploadRequest struct {
	SessionID uuid.UUID
}

func (s *blobStore) DeleteUpload(id uuid.UUID) error {
	req := deleteUploadRequest{
		SessionID: id,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := s.proposer.Propose(cluster.Request{
		Op:   opDeleteUpload,
		Data: data,
	})

	return resp.Err
}

func (s *blobStore) deleteUpload(req cluster.Request) (resp cluster.Response) {
	var deleteUploadRequest deleteUploadRequest
	err := json.Unmarshal(req.Data, &deleteUploadRequest)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = s.Blobs.DeleteUpload(deleteUploadRequest.SessionID)
	return
}
