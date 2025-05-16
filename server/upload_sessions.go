package server

import "net/http"

func (s *Server) blobsUploadsSessionHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.initUploadHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) initUploadHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	session, err := s.service.InitUpload(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.Header().Set(HeaderLocation, session.Location)
	w.WriteHeader(http.StatusAccepted)
}
