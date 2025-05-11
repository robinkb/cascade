package server

import (
	"encoding/json"
	"net/http"
	"strings"

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

	artifactType := r.URL.Query().Get("artifactType")

	opts := cascade.ListReferrersOptions{
		ArtifactType: artifactType,
	}

	referrers, err := s.service.ListReferrers(repository, digest, &opts)
	switch err {
	default:
		w.WriteHeader(http.StatusInternalServerError)
	case cascade.ErrDigestInvalid:
		w.WriteHeader(http.StatusBadRequest)
	case nil:
		w.Header().Set(HeaderContentType, v1.MediaTypeImageIndex)
		if len(referrers.AppliedFilters) > 0 {
			w.Header().Set(HeaderOCIFiltersApplied, strings.Join(referrers.AppliedFilters, ","))
		}
		json.NewEncoder(w).Encode(referrers.Index)
	}
}
