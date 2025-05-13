package server

import (
	"encoding/json"
	"net/http"
	"strconv"
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
	n := r.URL.Query().Get("n")
	last := r.URL.Query().Get("last")

	count := -1
	if n != "" {
		var err error
		count, err = strconv.Atoi(n)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	tags, _ := s.service.ListTags(repository, count, last)

	response := TagsListResponse{
		Name: repository,
		Tags: tags,
	}

	encodeOrLog(json.NewEncoder(w).Encode(response))
}
