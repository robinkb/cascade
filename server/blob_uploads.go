package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/robinkb/cascade-registry"
)

func (s *Server) blobsUploadsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.checkUploadHandler(w, r)
	case http.MethodPut:
		s.closeUploadHandler(w, r)
	case http.MethodPatch:
		if r.Header.Get(HeaderContentType) == ContentTypeOctetStream {
			if r.Header.Get(HeaderContentLength) != "" &&
				r.Header.Get(HeaderContentRange) != "" {
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

func (s *Server) checkUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	info, err := s.service.StatUpload(name, reference)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	// TODO: This is not the only place where this is generated.
	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, reference)

	w.Header().Set(HeaderLocation, location)
	w.Header().Set(HeaderRange, fmt.Sprintf("0-%d", max(info.Size-1, 0)))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) chunkedUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

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

	if err := s.service.AppendUpload(name, reference, bytes.NewBuffer(content), givenStart); err != nil {
		errorHandler(w, r, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, reference)
	w.Header().Set(HeaderLocation, location)
	w.Header().Set(HeaderRange, fmt.Sprintf("0-%d", givenEnd))
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) streamedUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	if err := s.service.AppendUpload(name, reference, r.Body, 0); err != nil {
		errorHandler(w, r, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, reference)
	w.Header().Set(HeaderLocation, location)
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) closeUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

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

		err = s.service.AppendUpload(name, reference, bytes.NewBuffer(content), offset)
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

	err := s.service.CloseUpload(name, reference, digest)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
	w.Header().Set(HeaderLocation, location)
	w.WriteHeader(http.StatusCreated)
}

func parseContentRange(r string) (start, end int64, err error) {
	err = cascade.ErrBlobUploadInvalid

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
