package cascade

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/opencontainers/go-digest"
)

var (
	ErrFileNotFound = errors.New("file not found")
)

type (
	RegistryStore interface {
		Stat(path string) (*FileInfo, error)
		Get(path string) ([]byte, error)
		Set(path string, content []byte) error
		// Reader(path string) (io.Reader, error)
		Put(path string, content []byte) error
		Delete(path string) error
		Move(sourcePath, destinationPath string)
	}

	MetadataStore interface {
		ListTags(repository string) ([]string, error)
		GetTag(repository, tag string) (digest.Digest, error)
		PutTag(repository, tag string, digest digest.Digest) error
		DeleteTag(repository, tag string) error
	}

	BlobStore interface {
		// Stat returns basic file info about the blob at the given path.
		Stat(path string) (*FileInfo, error)
		// Get returns the blob at the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		Get(path string) ([]byte, error)
		// Put writes content to the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		// Unlike Writer, Put does not append and always writes the entire blob.
		Put(path string, content []byte) error
		// Reader returns an io.Reader that can be used to read a blob.
		Reader(path string) (io.Reader, error)
		// Writer returns an io.Writer to write to a blob. Blobs are always appended to.
		// If a blob must be truncated, delete it first.
		Writer(path string) (io.Writer, error)
		// Delete removes the blob at the given path.
		Delete(path string) error
		// Move moves the blob from the source path to the destination path.
		// This may effectively be a rename on some backends.
		Move(sourcePath, destinationPath string) error
	}

	// Based (at least initially) on fs.FileInfo interface.
	FileInfo struct {
		Name string
		Size int64
	}
)

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		store: make(map[string][]byte),
	}
}

type InMemoryStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

func (s *InMemoryStore) Stat(path string) (*FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, ErrFileNotFound
	}

	return &FileInfo{
		Name: path,
		Size: int64(len(data)),
	}, nil
}

func (s *InMemoryStore) Get(path string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, ErrFileNotFound
	}
	return data, nil
}

// TODO: Currently this accepts 'nil' as the content,
// not sure if that is safe behavior.
func (s *InMemoryStore) Set(path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = content
	return nil
}

func (s *InMemoryStore) Put(path string, content []byte) error {
	_, err := s.Stat(path)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = append(s.store[path], content...)

	return nil
}

func (s *InMemoryStore) Delete(path string) error {
	_, err := s.Stat(path)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, path)
	return nil
}

func (s *InMemoryStore) Move(sourcePath, destinationPath string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[destinationPath] = s.store[sourcePath]
	delete(s.store, sourcePath)
}

func NewInMemoryBlobStore() BlobStore {
	return &InMemoryBlobStore{
		store: make(map[string][]byte),
	}
}

type InMemoryBlobStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

type writer struct {
	s    *InMemoryBlobStore
	path string
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()

	w.s.store[w.path] = append(w.s.store[w.path], p...)
	return len(p), nil
}

func (s *InMemoryBlobStore) Stat(path string) (*FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, ErrFileNotFound
	}

	return &FileInfo{
		Name: path,
		Size: int64(len(data)),
	}, nil
}

func (s *InMemoryBlobStore) Get(path string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, ErrFileNotFound
	}
	return data, nil
}

func (s *InMemoryBlobStore) Put(path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = content

	return nil
}

func (s *InMemoryBlobStore) Reader(path string) (io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return bytes.NewBuffer(s.store[path]), nil
}

func (s *InMemoryBlobStore) Writer(path string) (io.Writer, error) {
	return &writer{s, path}, nil
}

func (s *InMemoryBlobStore) Delete(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, path)
	return nil
}

func (s *InMemoryBlobStore) Move(sourcePath, destinationPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[destinationPath] = s.store[sourcePath]
	return nil
}
