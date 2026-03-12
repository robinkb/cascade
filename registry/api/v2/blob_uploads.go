package v2

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/robinkb/cascade-registry/repository"
)

func Location(name, reference string) string {
	return fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, reference)
}

func (h *Handler) blobsUploadsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.checkUploadHandler(w, r)
	case http.MethodPut:
		h.closeUploadHandler(w, r)
	case http.MethodPatch:
		if r.Header.Get(HeaderContentType) == ContentTypeOctetStream {
			if r.Header.Get(HeaderContentLength) != "" &&
				r.Header.Get(HeaderContentRange) != "" {
				h.chunkedUploadHandler(w, r)
			} else {
				h.streamedUploadHandler(w, r)
			}
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) checkUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	repo, err := h.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	info, err := repo.StatUpload(name, reference)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	location := Location(name, reference)

	w.Header().Set(HeaderLocation, location)
	w.Header().Set(HeaderRange, fmt.Sprintf("0-%d", max(info.Size-1, 0)))
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) chunkedUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	repo, err := h.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	givenStart, givenEnd, err := parseContentRange(r.Header.Get(HeaderContentRange))
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	expectedLength := givenEnd - givenStart

	content, err := io.ReadAll(r.Body)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	if int64(len(content)-1) != expectedLength {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if err := repo.AppendUpload(name, reference, bytes.NewBuffer(content), givenStart); err != nil {
		errorHandler(w, r, err)
		return
	}

	location := Location(name, reference)
	w.Header().Set(HeaderLocation, location)
	w.Header().Set(HeaderRange, fmt.Sprintf("0-%d", givenEnd))
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) streamedUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	repo, err := h.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	if err := repo.AppendUpload(name, reference, r.Body, 0); err != nil {
		errorHandler(w, r, err)
		return
	}

	location := Location(name, reference)
	w.Header().Set(HeaderLocation, location)
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) closeUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	repo, err := h.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	// This is either a monolithic upload, or closing a chunked upload
	// with a final chunk.
	if r.Body != nil && r.Body != http.NoBody {
		// Content-Type and Content-Length should be set if the request
		// contains a body.
		if r.Header.Get(HeaderContentType) != ContentTypeOctetStream ||
			r.Header.Get(HeaderContentLength) == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// If it's a chunked upload, Content-Range should have the offset.
		var offset int64
		if contentRange := r.Header.Get(HeaderContentRange); contentRange != "" {
			var err error
			offset, _, err = parseContentRange(contentRange)
			if err != nil {
				errorHandler(w, r, err)
				return
			}
		}

		content, err := io.ReadAll(r.Body)
		if err != nil {
			errorHandler(w, r, err)
			return
		}

		err = repo.AppendUpload(name, reference, bytes.NewBuffer(content), offset)
		if err != nil {
			errorHandler(w, r, err)
			return
		}
	}

	digest := r.URL.Query().Get("digest")
	if digest == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = repo.CloseUpload(name, reference, digest)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
	w.Header().Set(HeaderLocation, location)
	w.WriteHeader(http.StatusCreated)
}

func parseContentRange(r string) (start, end int64, err error) {
	err = repository.ErrBlobUploadInvalid

	parts := strings.Split(r, "-")
	if len(parts) != 2 {
		return
	}

	start, e := strconv.ParseInt(parts[0], 10, 64)
	if e != nil {
		return
	}

	end, e = strconv.ParseInt(parts[1], 10, 64)
	if e != nil {
		return
	}

	return start, end, nil
}
