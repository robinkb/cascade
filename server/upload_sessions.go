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
	repository := r.PathValue("repository")

	session := s.service.InitUpload(repository)
	w.Header().Set(headerLocation, session.Location)
	w.WriteHeader(http.StatusAccepted)
}
