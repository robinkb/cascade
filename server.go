package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	headerContentLength = "Content-Length"
	headerContentType   = "Content-Type"
	headerLocation      = "Location"

	contentTypeOctetStream = "application/octet-stream"
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
	registryRouter.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			return
		}

		segments := strings.Split(r.URL.Path, "/")
		i := len(segments) - 1
		for ; i > 0; i-- {
			if slices.Contains([]string{"blobs", "manifests", "tags", "referrers"}, segments[i]) {
				r.SetPathValue("repository", strings.Join(segments[1:i], "/"))
				break
			}
		}

		prefix := strings.Join(segments[:i], "/")
		http.StripPrefix(prefix, repositoryRouter).ServeHTTP(w, r)
	}))

	router := http.NewServeMux()
	router.Handle("/v2/", http.HandlerFunc(http.StripPrefix("/v2", registryRouter).ServeHTTP))
	// router.Handle("/v2/", registryRouter)

	s.Handler = router

	return s
}

type RegistryServer struct {
	service RegistryService
	http.Handler
}

func (s *RegistryServer) manifestsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		s.statManifestsHandler(w, r)
	case http.MethodGet:
		s.getManifestsHandler(w, r)
	case http.MethodPut:
		s.putManifestsHandler(w, r)
	case http.MethodDelete:
		s.deleteManifestsHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *RegistryServer) blobsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		s.statBlobsHandler(w, r)
	case http.MethodGet:
		s.getBlobsHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *RegistryServer) blobsUploadsSessionHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.initUploadHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *RegistryServer) blobsUploadsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		s.closeUploadHandler(w, r)
	case http.MethodPatch:
		s.writeUploadHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *RegistryServer) statManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	info, err := s.service.StatManifest(repository, reference)
	if err != nil {
		mapError(w, err)
		return
	}

	w.Header().Set(headerContentLength, strconv.Itoa(int(info.Size)))
	w.WriteHeader(http.StatusOK)
}

func (s *RegistryServer) getManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	// TODO: This is doing too much. GetManifest should verify the Manifest,
	// and return the media type.
	var manifest v1.Manifest
	content, err := s.service.GetManifest(repository, reference)
	if err != nil {
		mapError(w, err)
		return
	}
	json.Unmarshal(content, &manifest)

	w.Header().Set(headerContentType, manifest.MediaType)
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func (s *RegistryServer) putManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	// The stored manifest must be an exact byte representation.
	data, err := io.ReadAll(r.Body)
	if err != nil {
		mapError(w, err)
		return
	}
	err = s.service.PutManifest(repository, reference, data)
	if err != nil {
		mapError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *RegistryServer) deleteManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	err := s.service.DeleteManifest(repository, reference)
	if err != nil {
		if errors.Is(err, ErrManifestUnknown) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(NewErrorResponse(err.(Error)))
			return
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

func (s *RegistryServer) statBlobsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	digest := r.PathValue("digest")

	if _, err := s.service.StatBlob(repository, digest); err != nil {
		mapError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *RegistryServer) getBlobsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	digest := r.PathValue("digest")

	content, err := s.service.GetBlob(repository, digest)
	if err != nil {
		mapError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func (s *RegistryServer) initUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	session := s.service.InitUpload(repository)
	w.Header().Set(headerLocation, session.Location)
	w.WriteHeader(http.StatusAccepted)
}

func (s *RegistryServer) writeUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	content, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if err := s.service.WriteUpload(repository, reference, content); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, reference)
	w.Header().Set(headerLocation, location)
	w.WriteHeader(http.StatusAccepted)
}

func (s *RegistryServer) closeUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	_, err := s.service.StatUpload(repository, reference)
	if err != nil {
		mapError(w, err)
		return
	}

	// This is either a monolithic upload, or closing a chunked upload
	// with a final chunk.
	if r.Body != nil {
		// Content-Type and Content-Length should be set if the request
		// contains a body.
		if r.Header.Get(headerContentType) != contentTypeOctetStream ||
			r.Header.Get(headerContentLength) == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		content, _ := io.ReadAll(r.Body)
		// TODO: Check this error
		s.service.WriteUpload(repository, reference, content)
	}

	digest := r.URL.Query().Get("digest")
	if digest == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = s.service.CloseUpload(repository, reference, digest)
	if err != nil {
		if errors.Is(err, ErrDigestInvalid) {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/%s", repository, digest)
	w.Header().Set(headerLocation, location)
	w.WriteHeader(http.StatusCreated)
}

// This is starting to feel like the wrong approach.
// ErrDigestInvalid should definitely not result in a 404 in most cases.
func mapError(w http.ResponseWriter, err error) {
	var response *ErrorResponse
	code := http.StatusInternalServerError

	switch {
	case errors.Is(err, ErrBlobUnknown):
		code = http.StatusNotFound
		response = NewErrorResponse(err.(Error))
	case errors.Is(err, ErrBlobUploadUnknown):
		code = http.StatusNotFound
		response = NewErrorResponse(err.(Error))
	case errors.Is(err, ErrManifestUnknown):
		code = http.StatusNotFound
		response = NewErrorResponse(err.(Error))
	case errors.Is(err, ErrDigestInvalid):
		code = http.StatusNotFound
		response = NewErrorResponse(err.(Error))
	}

	w.WriteHeader(code)
	json.NewEncoder(w).Encode(response)
}
