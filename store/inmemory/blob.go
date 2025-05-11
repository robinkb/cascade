package inmemory

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
)

const (
	blobPathFormat   = "blobs/%s/%s/%s"
	uploadPathFormat = "uploads/%s"
)

func NewBlobStore() cascade.BlobStore {
	return &blobStore{
		store: make(map[string][]byte),
	}
}

type blobStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

type writer struct {
	s    *blobStore
	path string
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()

	w.s.store[w.path] = append(w.s.store[w.path], p...)
	return len(p), nil
}

func (s *blobStore) StatBlob(id digest.Digest) (*cascade.FileInfo, error) {
	path := s.digestToPath(id)
	return s.stat(path)
}

func (s *blobStore) StatUpload(id uuid.UUID) (*cascade.FileInfo, error) {
	path := s.uuidToPath(id)
	return s.stat(path)
}

func (s *blobStore) Get(path string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, cascade.ErrFileNotFound
	}
	return data, nil
}

func (s *blobStore) Put(path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = content

	return nil
}

func (s *blobStore) Reader(path string) (io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return bytes.NewBuffer(s.store[path]), nil
}

func (s *blobStore) Writer(path string) (io.Writer, error) {
	return &writer{s, path}, nil
}

func (s *blobStore) Delete(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, path)
	return nil
}

func (s *blobStore) Move(sourcePath, destinationPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[destinationPath] = s.store[sourcePath]
	return nil
}

func (s *blobStore) digestToPath(id digest.Digest) string {
	return fmt.Sprintf(blobPathFormat, id.Algorithm(), id.Encoded()[0:2], id.Encoded())
}

func (s *blobStore) uuidToPath(id uuid.UUID) string {
	return fmt.Sprintf(uploadPathFormat, id.String())
}

func (s *blobStore) stat(path string) (*cascade.FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, cascade.ErrFileNotFound
	}

	return &cascade.FileInfo{
		Name: path,
		Size: int64(len(data)),
	}, nil
}
