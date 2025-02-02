package server

import (
	"encoding/json"
	"net/http"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
)

func (s *Server) referrersHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listReferrersHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) listReferrersHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	digest := r.PathValue("digest")

	index, err := s.service.ListReferrers(repository, digest)
	switch err {
	default:
		w.WriteHeader(http.StatusInternalServerError)
	case cascade.ErrDigestInvalid:
		w.WriteHeader(http.StatusBadRequest)
	case nil:
		w.Header().Add(headerContentType, v1.MediaTypeImageIndex)
		json.NewEncoder(w).Encode(index)
	}
}
