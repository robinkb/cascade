package v2

import (
	"encoding/json"
	"net/http"
	"strconv"

	v1 "github.com/opencontainers/distribution-spec/specs-go/v1"
)

func (h *Handler) handleCatalog(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleListRepositories(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleListRepositories(w http.ResponseWriter, r *http.Request) {
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

	repositories, err := h.service.ListRepositories(count, last)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	response := v1.RepositoryList{
		Repositories: repositories,
	}

	encodeOrLog(json.NewEncoder(w).Encode(response))
}
