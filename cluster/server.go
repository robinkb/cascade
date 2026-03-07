package cluster

import "net/http"

func NewServer() *Server {
	return &Server{
		router: http.NewServeMux(),
	}
}

// Server provides a pluggable server for cluster operations.
// Not sure if there is any value to this atm tbh
type Server struct {
	router *http.ServeMux
}

func (s *Server) Handle(pattern string, handler http.Handler) {
	s.router.Handle(pattern, handler)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
