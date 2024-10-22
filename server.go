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

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	headerContentLength = "Content-Length"
	headerContentRange  = "Content-Range"
	headerContentType   = "Content-Type"
	headerLocation      = "Location"
	headerRange         = "Range"

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
	case http.MethodGet:
		s.checkUploadHandler(w, r)
	case http.MethodPut:
		s.closeUploadHandler(w, r)
	case http.MethodPatch:
		if r.Header.Get(headerContentType) == contentTypeOctetStream {
			if r.Header.Get(headerContentLength) != "" &&
				r.Header.Get(headerContentRange) != "" {
				s.chunkedUploadHandler(w, r)
			} else {
				s.streamedUploadHandler(w, r)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *RegistryServer) statManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	info, err := s.service.StatManifest(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	w.Header().Set(headerContentLength, strconv.Itoa(int(info.Size)))
	w.WriteHeader(http.StatusOK)
}

func (s *RegistryServer) getManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	// If the reference is a tag, fetch the digest first.
	if validateTag(reference) {
		reference, _ = s.service.GetTag(repository, reference)
	}

	// TODO: This is doing too much. GetManifest should verify the Manifest,
	// and return the media type.
	var manifest v1.Manifest
	content, err := s.service.GetManifest(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
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

	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	digest := digest.FromBytes(data)

	if !validateTag(reference) {
		if digest.String() != reference {
			writeErrorResponse(w, ErrDigestInvalid)
			return
		}
	}

	err = s.service.PutManifest(repository, digest.String(), data)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	if validateTag(reference) {
		err = s.service.PutTag(repository, reference, digest.String())
		if err != nil {
			writeErrorResponse(w, err)
			return
		}
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
		writeErrorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *RegistryServer) getBlobsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	digest := r.PathValue("digest")

	content, err := s.service.GetBlob(repository, digest)
	if err != nil {
		writeErrorResponse(w, err)
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

func (s *RegistryServer) checkUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	info, err := s.service.StatUpload(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	// TODO: This is not the only place where this is generated.
	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, reference)

	w.Header().Set(headerLocation, location)
	w.Header().Set(headerRange, fmt.Sprintf("0-%d", info.Size))
	w.WriteHeader(http.StatusNoContent)
}

func (s *RegistryServer) chunkedUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	info, err := s.service.StatUpload(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	givenStart, givenEnd, err := parseContentRange(r.Header.Get(headerContentRange))
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	if info.Size != int64(givenStart) {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	content, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	if len(content) != givenEnd-givenStart {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if err := s.service.AppendUpload(repository, reference, content); err != nil {
		writeErrorResponse(w, err)
		return
	}

	info, err = s.service.StatUpload(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, reference)
	w.Header().Set(headerLocation, location)
	w.Header().Set(headerRange, fmt.Sprintf("0-%d", info.Size))
	w.WriteHeader(http.StatusAccepted)
}

func (s *RegistryServer) streamedUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	_, err := s.service.StatUpload(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	content, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	if err := s.service.AppendUpload(repository, reference, content); err != nil {
		writeErrorResponse(w, err)
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
		writeErrorResponse(w, err)
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
		s.service.AppendUpload(repository, reference, content)
	}

	digest := r.URL.Query().Get("digest")
	if digest == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = s.service.CloseUpload(repository, reference, digest)
	if err != nil {
		if errors.Is(err, ErrDigestInvalid) {
			err = ErrBlobUploadInvalid
		}
		writeErrorResponse(w, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/%s", repository, digest)
	w.Header().Set(headerLocation, location)
	w.WriteHeader(http.StatusCreated)
}

func parseContentRange(r string) (start, end int, err error) {
	err = ErrBlobUploadInvalid

	parts := strings.Split(r, "-")
	if len(parts) != 2 {
		return
	}

	start, e := strconv.Atoi(parts[0])
	if e != nil {
		return
	}

	end, e = strconv.Atoi(parts[1])
	if e != nil {
		return
	}

	return start, end, nil
}

// This is starting to feel like the wrong approach.
// ErrDigestInvalid should definitely not result in a 404 in most cases.
func writeErrorResponse(w http.ResponseWriter, err error) {
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
	case errors.Is(err, ErrBlobUploadInvalid):
		code = http.StatusBadRequest
		response = NewErrorResponse(err.(Error))
	}

	w.WriteHeader(code)
	json.NewEncoder(w).Encode(response)
}
