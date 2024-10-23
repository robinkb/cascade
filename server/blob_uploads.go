package server

import (
	"errors"
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
	w.Header().Set(headerRange, fmt.Sprintf("0-%d", info.Size))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) chunkedUploadHandler(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) streamedUploadHandler(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) closeUploadHandler(w http.ResponseWriter, r *http.Request) {
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
		if errors.Is(err, cascade.ErrDigestInvalid) {
			err = cascade.ErrBlobUploadInvalid
		}
		writeErrorResponse(w, err)
		return
	}

	location := fmt.Sprintf("/v2/%s/blobs/%s", repository, digest)
	w.Header().Set(headerLocation, location)
	w.WriteHeader(http.StatusCreated)
}

func parseContentRange(r string) (start, end int, err error) {
	err = cascade.ErrBlobUploadInvalid

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
