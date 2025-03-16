package server

import (
	"io"
	"net/http"
	"strconv"
)

func (s *Server) blobsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		s.statBlobsHandler(w, r)
	case http.MethodGet:
		s.getBlobsHandler(w, r)
	case http.MethodDelete:
		s.deleteBlobsHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) statBlobsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	digest := r.PathValue("digest")

	info, err := s.service.StatBlob(repository, digest)
	if err != nil {
		// TODO: This currently writes a body, even though HEAD responses
		// should never write a body. Something to consider when this gets refactored.
		writeErrorResponse(w, err)
		return
	}

	w.Header().Set(HeaderContentLength, strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getBlobsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	digest := r.PathValue("digest")

	blob, err := s.service.GetBlob(repository, digest)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	io.Copy(w, blob)
}

func (s *Server) deleteBlobsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	digest := r.PathValue("digest")

	err := s.service.DeleteBlob(repository, digest)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
