package api

import (
	"io"
	"net/http"

	godigest "github.com/opencontainers/go-digest"

	"github.com/robinkb/cascade/registry/store"
)

func New(blobs store.BlobReader) *Handler {
	h := new(Handler)

	h.blobs = blobs

	mux := http.NewServeMux()
	mux.HandleFunc("/blobs/{digest}", h.blobHandler)

	h.Handler = mux

	return h
}

type Handler struct {
	http.Handler
	blobs store.BlobReader
}

func (h *Handler) blobHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.getBlobHandler(w, r)
	default:
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) getBlobHandler(w http.ResponseWriter, r *http.Request) {
	digest := r.PathValue("digest")

	id, err := godigest.Parse(digest)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	rd, err := h.blobs.BlobReader(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err = io.Copy(w, rd)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
