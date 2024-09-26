package main

import (
	"encoding/json"
	"net/http"
	"strings"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type (
	RegistryStore interface {
		GetBlob(digest string) []byte
	}
)

func NewRegistryServer(store RegistryStore) *RegistryServer {
	s := new(RegistryServer)

	s.store = store

	router := http.NewServeMux()
	router.Handle("/v2/library/fedora/manifests/", http.HandlerFunc(s.manifestsHandler))
	router.Handle("/v2/library/fedora/blobs/", http.HandlerFunc(s.blobsHandler))
	s.Handler = router

	return s
}

type RegistryServer struct {
	store RegistryStore
	http.Handler
}

func (s *RegistryServer) manifestsHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(v1.Manifest{})
}

func (s *RegistryServer) blobsHandler(w http.ResponseWriter, r *http.Request) {
	digest := strings.TrimPrefix(r.URL.Path, "/v2/library/fedora/blobs/")

	// TODO: This should probably be refactored to write directly to w,
	// because this code buffers blobs into memory.
	blob := s.store.GetBlob(digest)

	if len(blob) == 0 {
		w.WriteHeader(http.StatusNotFound)
	}

	if r.Method != http.MethodHead {
		w.Write(s.store.GetBlob(digest))
	}
}
