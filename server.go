package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type (
	RegistryStore interface {
		StatBlob(name, digest string) bool
		GetBlob(name, digest string) io.Reader
		WriteBlob(name string, r io.Reader) string
		StatManifest(name, reference string) (bool, int)
		GetManifest(name, reference string) []byte
		PutManifest(name, reference string, data []byte)
		InitUploadSession(name string) *UploadSession
		ActiveUploadSession(name, id string) bool
	}

	UploadSession struct {
		ID, Location string
	}
)

func NewRegistryServer(store RegistryStore) *RegistryServer {
	s := new(RegistryServer)

	s.store = store

	repositoryRouter := http.NewServeMux()
	repositoryRouter.Handle("/manifests/{reference}", http.HandlerFunc(s.manifestsHandler))
	repositoryRouter.Handle("/blobs/{digest}", http.HandlerFunc(s.blobsHandler))
	repositoryRouter.Handle("/blobs/uploads/", http.HandlerFunc(s.blobsUploadsSessionHandler))
	repositoryRouter.Handle("/blobs/uploads/{reference}", http.HandlerFunc(s.blobsUploadsHandler))

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
			return
		}
		w.WriteHeader(http.StatusNotFound)

	case http.MethodGet:
		var manifest v1.Manifest
		data := s.store.GetManifest(name, reference)
		if data == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		json.Unmarshal(data, &manifest)

		w.Header().Set("Content-Type", manifest.MediaType)
		w.WriteHeader(http.StatusOK)
		w.Write(data)

	case http.MethodPut:
		// The stored manifest must be an exact byte representation.
		data, _ := io.ReadAll(r.Body)
		s.store.PutManifest(name, reference, data)
		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *RegistryServer) blobsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	digest := r.PathValue("digest")

	if !s.store.StatBlob(name, digest) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if r.Method != http.MethodHead {
		io.Copy(w, s.store.GetBlob(name, digest))
	}
}

func (s *RegistryServer) blobsUploadsSessionHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	switch r.Method {
	case http.MethodPost:
		session := s.store.InitUploadSession(name)
		w.Header().Set("Location", session.Location)
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *RegistryServer) blobsUploadsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	fmt.Printf("%s", r.Header.Get("Content-Length"))

	switch r.Method {
	case http.MethodPut:
		if !s.store.ActiveUploadSession(name, reference) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if r.Body == nil ||
			r.Header.Get("Content-Type") != "application/octet-stream" ||
			r.Header.Get("Content-Length") == "" ||
			r.URL.Query().Get("digest") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		digest := s.store.WriteBlob(name, r.Body)
		if digest != r.URL.Query().Get("digest") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		location := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
		w.Header().Set("Location", location)
		w.WriteHeader(http.StatusCreated)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
