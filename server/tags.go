package server

import (
	"encoding/json"
	"net/http"
)

func (s *Server) tagsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listTagsHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) listTagsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")

	tags, _ := s.service.ListTags(repository)

	response := TagsListResponse{
		Name: repository,
		Tags: tags,
	}

	json.NewEncoder(w).Encode(response)
}
