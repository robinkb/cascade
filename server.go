package main

import (
	"net/http"
	"strings"
)

type (
	RegistryStore interface {
		GetBlob(digest string) []byte
	}
)

type RegistryServer struct {
	store RegistryStore
}

func (s *RegistryServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	digest := strings.TrimPrefix(r.URL.Path, "/v2/library/fedora/blobs/")

	// TODO: This should probably be refactored to write directly to w,
	// because this code buffers blobs into memory.
	blob := s.store.GetBlob(digest)

	if len(blob) == 0 {
		w.WriteHeader(http.StatusNotFound)
	}

	w.Write(s.store.GetBlob(digest))
}
