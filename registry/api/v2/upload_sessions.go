package v2

import "net/http"

func (h *Handler) blobsUploadsSessionHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.initUploadHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) initUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	repository, err := h.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	session, err := repository.InitUpload()
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	location := Location(name, session.ID.String())

	w.Header().Set(HeaderLocation, location)
	w.WriteHeader(http.StatusAccepted)
}
