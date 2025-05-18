package server

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"slices"
	"strings"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/repository"
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
				r.SetPathValue("name", strings.Join(segments[1:i], "/"))
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

func errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	var response *ErrorResponse
	var cerr repository.Error
	code := http.StatusInternalServerError

	if errors.As(err, &cerr) {
		switch {
		// Standard Distribution errors
		case errors.Is(cerr, repository.ErrBlobUnknown):
			code = http.StatusNotFound
		case errors.Is(cerr, repository.ErrBlobUploadInvalid):
			code = http.StatusBadRequest
		case errors.Is(cerr, repository.ErrBlobUploadUnknown):
			code = http.StatusNotFound
		case errors.Is(cerr, repository.ErrDigestInvalid):
			code = http.StatusBadRequest
		case errors.Is(cerr, repository.ErrManifestBlobUnknown):
			code = http.StatusNotFound
		case errors.Is(cerr, repository.ErrManifestInvalid):
			code = http.StatusBadRequest
		case errors.Is(cerr, repository.ErrManifestUnknown):
			code = http.StatusNotFound
		case errors.Is(cerr, repository.ErrNameInvalid):
			code = http.StatusBadRequest
		case errors.Is(cerr, repository.ErrNameUnknown):
			code = http.StatusNotFound
		case errors.Is(cerr, repository.ErrSizeInvalid):
			code = http.StatusBadRequest
		case errors.Is(cerr, repository.ErrUnauthorized):
			code = http.StatusUnauthorized
		case errors.Is(cerr, repository.ErrDenied):
			code = http.StatusForbidden
		case errors.Is(cerr, repository.ErrUnsupported):
			code = http.StatusNotFound
		case errors.Is(cerr, repository.ErrTooManyRequests):
			code = http.StatusTooManyRequests
		// Extra errors
		case errors.Is(cerr, repository.ErrTagInvalid):
			code = http.StatusBadRequest
		case errors.Is(cerr, repository.ErrUploadOffsetInvalid):
			code = http.StatusRequestedRangeNotSatisfiable
		}

		response = NewErrorResponse(cerr)
	}

	w.WriteHeader(code)
	if r.Method != http.MethodHead {
		encodeOrLog(json.NewEncoder(w).Encode(response))
	}
}

func NewErrorResponse(err ...repository.Error) *ErrorResponse {
	return &ErrorResponse{
		Errors: err,
	}
}

type ErrorResponse struct {
	Errors []repository.Error `json:"errors"`
}

func (e ErrorResponse) Error() string {
	errs := make([]string, len(e.Errors))
	for i := range e.Errors {
		errs[i] = e.Errors[i].Error()
	}

	return strings.Join(errs, ", ")
}

func writeOrLog(_ any, err error) {
	if err != nil {
		log.Printf("error writing content to client: %s", err)
	}
}

func encodeOrLog(err error) {
	if err != nil {
		log.Printf("error writing encoded content to client: %s", err)
	}
}
