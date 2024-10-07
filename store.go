package main

import (
	"errors"
	"sync"
)

var (
	ErrFileNotFound = errors.New("file not found")
)

type (
	RegistryStore interface {
		Stat(path string) (*FileInfo, error)
		Get(path string) ([]byte, error)
		Put(path string, content []byte) error
		// Reader(path string) (io.Reader, error)
		// Writer(path string) (io.Writer, error)
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

func (s *InMemoryStore) Put(path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = content
	return nil
}
