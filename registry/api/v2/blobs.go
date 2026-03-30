package v2

import (
	"io"
	"net/http"
	"strconv"
)

func (h *Handler) blobsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		h.statBlobsHandler(w, r)
	case http.MethodGet:
		h.getBlobsHandler(w, r)
	case http.MethodDelete:
		h.deleteBlobsHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) statBlobsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	digest := r.PathValue("digest")

	repo, err := h.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	info, err := repo.StatBlob(digest)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.Header().Set(HeaderContentLength, strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getBlobsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	digest := r.PathValue("digest")

	repo, err := h.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	blob, err := repo.GetBlob(digest)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	writeOrLog(io.Copy(w, blob))
}

func (h *Handler) deleteBlobsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	digest := r.PathValue("digest")

	repo, err := h.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	err = repo.DeleteBlob(digest)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
