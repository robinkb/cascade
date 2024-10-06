package main

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

type RegistryStore interface {
	Stat(path string) bool
	Get(path string) (io.Reader, error)
	Put(path string, r io.Reader) error
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		store: make(map[string][]byte),
	}
}

type InMemoryStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

func (s *InMemoryStore) Stat(path string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.store[path]
	return ok
}

func (s *InMemoryStore) Get(path string) (io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, errors.New("file not found")
	}
	return bytes.NewBuffer(data), nil
}

func (s *InMemoryStore) Put(path string, r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := bytes.NewBuffer([]byte{})
	_, err := io.Copy(buf, r)
	if err != nil {
		return err
	}

	s.store[path] = buf.Bytes()
	return nil
}
