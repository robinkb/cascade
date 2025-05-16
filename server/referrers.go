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
	name := r.PathValue("name")
	digest := r.PathValue("digest")
	artifactType := r.URL.Query().Get("artifactType")

	repository, err := s.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	opts := cascade.ListReferrersOptions{
		ArtifactType: artifactType,
	}

	referrers, err := repository.ListReferrers(name, digest, &opts)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.Header().Set(HeaderContentType, v1.MediaTypeImageIndex)
	if len(referrers.AppliedFilters) > 0 {
		w.Header().Set(HeaderOCIFiltersApplied, strings.Join(referrers.AppliedFilters, ","))
	}
	encodeOrLog(json.NewEncoder(w).Encode(referrers.Index))
}
