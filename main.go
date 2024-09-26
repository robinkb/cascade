package main

import (
	"log"
	"net/http"
)

type InMemoryRegistryStore struct{}

func (s *InMemoryRegistryStore) GetBlob(digest string) []byte {
	return []byte("123")
}

func main() {
	store := &InMemoryRegistryStore{}
	server := &RegistryServer{store}
	log.Fatal(http.ListenAndServe(":5000", server))
}
