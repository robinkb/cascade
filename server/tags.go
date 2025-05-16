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
	name := r.PathValue("name")
	n := r.URL.Query().Get("n")
	last := r.URL.Query().Get("last")

	repository, err := s.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	count := -1
	if n != "" {
		var err error
		count, err = strconv.Atoi(n)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	tags, err := repository.ListTags(name, count, last)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	response := TagsListResponse{
		Name: name,
		Tags: tags,
	}

	encodeOrLog(json.NewEncoder(w).Encode(response))
}
