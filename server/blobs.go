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
	name := r.PathValue("name")
	digest := r.PathValue("digest")

	info, err := s.service.StatBlob(name, digest)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.Header().Set(HeaderContentLength, strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getBlobsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	digest := r.PathValue("digest")

	blob, err := s.service.GetBlob(name, digest)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	writeOrLog(io.Copy(w, blob))
}

func (s *Server) deleteBlobsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	digest := r.PathValue("digest")

	err := s.service.DeleteBlob(name, digest)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
