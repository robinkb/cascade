package main

import (
	"log"
	"net/http"
)

type InMemoryRegistryStore struct{}

func (s *InMemoryRegistryStore) StatBlob(name, digest string) bool {
	return true
}

func (s *InMemoryRegistryStore) GetBlob(name, digest string) []byte {
	return []byte("123")
}

func (s *InMemoryRegistryStore) StatManifest(name, reference string) (bool, int) {
	return true, 3
}

func (s *InMemoryRegistryStore) GetManifest(name, reference string) []byte {
	return []byte("123")
}

func main() {
	store := &InMemoryRegistryStore{}
	server := NewRegistryServer(store)
	log.Fatal(http.ListenAndServe(":5000", server))
}
