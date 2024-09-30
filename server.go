package main

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type (
	RegistryStore interface {
		StatBlob(name, digest string) bool
		GetBlob(name, digest string) []byte
		StatManifest(name, reference string) (bool, int)
	}
)

func NewRegistryServer(store RegistryStore) *RegistryServer {
	s := new(RegistryServer)

	s.store = store

	repositoryRouter := http.NewServeMux()
	repositoryRouter.Handle("/manifests/{reference}", http.HandlerFunc(s.manifestsHandler))
	repositoryRouter.Handle("/blobs/{digest}", http.HandlerFunc(s.blobsHandler))

	registryRouter := http.NewServeMux()
	registryRouter.Handle("/v2/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		segments := strings.Split(r.URL.Path, "/")
		i := len(segments) - 1
		for ; i > 0; i-- {
			if slices.Contains([]string{"blobs", "manifests", "tags", "referrers"}, segments[i]) {
				r.SetPathValue("name", strings.Join(segments[2:i], "/"))
				break
			}
		}

		prefix := strings.Join(segments[:i], "/")
		http.StripPrefix(prefix, repositoryRouter).ServeHTTP(w, r)
	}))

	router := http.NewServeMux()
	router.Handle("/v2/", registryRouter)

	s.Handler = router

	return s
}

type RegistryServer struct {
	store RegistryStore
	http.Handler
}

func (s *RegistryServer) manifestsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	switch r.Method {
	case http.MethodHead:
		if ok, len := s.store.StatManifest(name, reference); ok {
			w.Header().Set("Content-Length", strconv.Itoa(len))
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}

	case http.MethodGet:
		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(v1.Manifest{})
	case http.MethodPut:
		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		w.WriteHeader(http.StatusAccepted)
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (s *RegistryServer) blobsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	digest := r.PathValue("digest")

	if !s.store.StatBlob(name, digest) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if r.Method != http.MethodHead {
		// TODO: This should probably be refactored to write directly to w,
		// because this code buffers blobs into memory.
		w.Write(s.store.GetBlob(name, digest))
	}
}
