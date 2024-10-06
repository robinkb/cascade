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

func NewRegistryServer(service RegistryService) *RegistryServer {
	s := new(RegistryServer)

	s.service = service

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
	service RegistryService
	http.Handler
}

func (s *RegistryServer) manifestsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	switch r.Method {
	case http.MethodHead:
		if ok, len := s.service.StatManifest(name, reference); ok {
			w.Header().Set("Content-Length", strconv.Itoa(len))
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)

	case http.MethodGet:
		var manifest v1.Manifest
		data := s.service.GetManifest(name, reference)
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
		s.service.PutManifest(name, reference, data)
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

	if !s.service.StatBlob(name, digest) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if r.Method != http.MethodHead {
		io.Copy(w, s.service.GetBlob(name, digest))
	}
}

func (s *RegistryServer) blobsUploadsSessionHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	switch r.Method {
	case http.MethodPost:
		session := s.service.InitUploadSession(name)
		w.Header().Set("Location", session.Location)
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *RegistryServer) blobsUploadsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	switch r.Method {
	case http.MethodPut:
		if !s.service.ActiveUploadSession(name, reference) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if r.Body == nil ||
			r.Header.Get("Content-Type") != "application/octet-stream" ||
			r.Header.Get("Content-Length") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		digest := r.URL.Query().Get("digest")
		err := s.service.WriteBlob(name, digest, r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// TODO: HTTP Handler shouldn't have to know how to construct the location.
		location := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
		w.Header().Set("Location", location)
		w.WriteHeader(http.StatusCreated)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
