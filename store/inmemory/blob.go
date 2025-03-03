package inmemory

import (
	"bytes"
	"io"
	"sync"

	"github.com/robinkb/cascade-registry"
)

func NewBlobStore() cascade.BlobStore {
	return &BlobStore{
		store: make(map[string][]byte),
	}
}

type BlobStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

type writer struct {
	s    *BlobStore
	path string
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()

	w.s.store[w.path] = append(w.s.store[w.path], p...)
	return len(p), nil
}

func (s *BlobStore) Stat(path string) (*cascade.FileInfo, error) {
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

func (s *BlobStore) Get(path string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, cascade.ErrFileNotFound
	}
	return data, nil
}

func (s *BlobStore) Put(path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = content

	return nil
}

func (s *BlobStore) Reader(path string) (io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return bytes.NewBuffer(s.store[path]), nil
}

func (s *BlobStore) Writer(path string) (io.Writer, error) {
	return &writer{s, path}, nil
}

func (s *BlobStore) Delete(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, path)
	return nil
}

func (s *BlobStore) Move(sourcePath, destinationPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[destinationPath] = s.store[sourcePath]
	return nil
}
