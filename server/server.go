package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/robinkb/cascade-registry"
)

const (
	// Standard headers
	HeaderContentLength = "Content-Length"
	HeaderContentRange  = "Content-Range"
	HeaderContentType   = "Content-Type"
	HeaderLocation      = "Location"
	HeaderRange         = "Range"

	// Distribution headers
	HeaderOCIFiltersApplied = "OCI-Filters-Applied"
	HeaderOCISubject        = "OCI-Subject"

	// Standard content types
	ContentTypeOctetStream = "application/octet-stream"
)

func New(service cascade.RegistryService) *Server {
	s := new(Server)

	s.service = service

	repositoryRouter := http.NewServeMux()
	repositoryRouter.Handle("/blobs/{digest}", http.HandlerFunc(s.blobsHandler))
	repositoryRouter.Handle("/blobs/uploads/", http.HandlerFunc(s.blobsUploadsSessionHandler))
	repositoryRouter.Handle("/blobs/uploads/{reference}", http.HandlerFunc(s.blobsUploadsHandler))
	repositoryRouter.Handle("/manifests/{reference}", http.HandlerFunc(s.manifestsHandler))
	repositoryRouter.Handle("/tags/list", http.HandlerFunc(s.tagsHandler))
	repositoryRouter.Handle("/referrers/{digest}", http.HandlerFunc(s.referrersHandler))

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

type (
	Server struct {
		service cascade.RegistryService
		http.Handler
	}

	TagsListResponse struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
)

// This is starting to feel like the wrong approach.
// ErrDigestInvalid should definitely not result in a 404 in most cases.
func writeErrorResponse(w http.ResponseWriter, err error) {
	var response *ErrorResponse
	code := http.StatusInternalServerError

	switch {
	case errors.Is(err, cascade.ErrBlobUnknown):
		code = http.StatusNotFound
		response = NewErrorResponse(err.(cascade.Error))
	case errors.Is(err, cascade.ErrBlobUploadUnknown):
		code = http.StatusNotFound
		response = NewErrorResponse(err.(cascade.Error))
	case errors.Is(err, cascade.ErrManifestUnknown):
		code = http.StatusNotFound
		response = NewErrorResponse(err.(cascade.Error))
	case errors.Is(err, cascade.ErrManifestInvalid):
		code = http.StatusBadRequest
		response = NewErrorResponse(err.(cascade.Error))
	case errors.Is(err, cascade.ErrDigestInvalid):
		code = http.StatusBadRequest
		response = NewErrorResponse(err.(cascade.Error))
	case errors.Is(err, cascade.ErrBlobUploadInvalid):
		code = http.StatusBadRequest
		response = NewErrorResponse(err.(cascade.Error))
	case errors.Is(err, cascade.ErrUploadOffsetInvalid):
		code = http.StatusRequestedRangeNotSatisfiable
		response = NewErrorResponse(err.(cascade.Error))
	}

	w.WriteHeader(code)
	json.NewEncoder(w).Encode(response)
}

func NewErrorResponse(err ...cascade.Error) *ErrorResponse {
	return &ErrorResponse{
		Errors: err,
	}
}

type ErrorResponse struct {
	Errors []cascade.Error `json:"errors"`
}

func (e ErrorResponse) Error() string {
	errs := make([]string, len(e.Errors))
	for i := range e.Errors {
		errs[i] = e.Errors[i].Error()
	}

	return strings.Join(errs, ", ")
}
