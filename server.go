package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
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
