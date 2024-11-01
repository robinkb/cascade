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

func (s *Server) checkUploadHandler(w http.ResponseWriter, r *http.Request) {
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
	w.Header().Set(headerRange, fmt.Sprintf("0-%d", max(info.Size-1, 0)))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) chunkedUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	givenStart, givenEnd, err := parseContentRange(r.Header.Get(headerContentRange))
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	expectedLength := givenEnd - givenStart

	content, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	if int64(len(content)-1) != expectedLength {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if err := s.service.AppendUpload(repository, reference, bytes.NewBuffer(content), givenStart); err != nil {
		writeErrorResponse(w, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, reference)
	w.Header().Set(headerLocation, location)
	w.Header().Set(headerRange, fmt.Sprintf("0-%d", givenEnd))
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) streamedUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	if err := s.service.AppendUpload(repository, reference, r.Body, 0); err != nil {
		writeErrorResponse(w, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, reference)
	w.Header().Set(headerLocation, location)
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) closeUploadHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	// This is either a monolithic upload, or closing a chunked upload
	// with a final chunk.
	if r.Body != nil && r.Body != http.NoBody {
		// Content-Type and Content-Length should be set if the request
		// contains a body.
		if r.Header.Get(headerContentType) != contentTypeOctetStream ||
			r.Header.Get(headerContentLength) == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// If it's a chunked upload, Content-Range should have the offset.
		var offset int64
		if contentRange := r.Header.Get(headerContentRange); contentRange != "" {
			var err error
			offset, _, err = parseContentRange(contentRange)
			if err != nil {
				writeErrorResponse(w, err)
				return
			}
		}

		content, _ := io.ReadAll(r.Body)
		// TODO: Check this error
		s.service.AppendUpload(repository, reference, bytes.NewBuffer(content), offset)
	}

	digest := r.URL.Query().Get("digest")
	if digest == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err := s.service.CloseUpload(repository, reference, digest)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/%s", repository, digest)
	w.Header().Set(headerLocation, location)
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
