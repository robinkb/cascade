package main

import "net/http"

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
