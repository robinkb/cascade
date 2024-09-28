package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type (
	RegistryStore interface {
		BlobExists(name, digest string) bool
		GetBlob(name, digest string) []byte
	}
)

func NewRegistryServer(store RegistryStore) *RegistryServer {
	s := new(RegistryServer)

	s.store = store

	router := http.NewServeMux()
	router.Handle("/v2/{group}/{repository}/manifests/{reference}", http.HandlerFunc(s.manifestsHandler))
	router.Handle("/v2/{group}/{repository}/blobs/{digest}", http.HandlerFunc(s.blobsHandler))
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
	group := r.PathValue("group")
	repository := r.PathValue("repository")
	digest := r.PathValue("digest")

	name := fmt.Sprintf("%s/%s", group, repository)

	if !s.store.BlobExists(name, digest) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if r.Method != http.MethodHead {
		// TODO: This should probably be refactored to write directly to w,
		// because this code buffers blobs into memory.
		w.Write(s.store.GetBlob(name, digest))
	}
}
