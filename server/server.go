package server

import (
	"context"
	"net"
	"net/http"
	"time"
)

type ServerOptions struct {
	Name            string
	Addr            net.Addr
	ShutdowmTimeout time.Duration
}

func NewServer(opts ServerOptions) *Server {
	mux := http.NewServeMux()
	srv := &Server{
		srv: &http.Server{
			Handler: mux,
		},
		mux: mux,
	}

	srv.name = opts.Name

	if opts.Addr != nil {
		srv.srv.Addr = opts.Addr.String()
	}

	if opts.ShutdowmTimeout != 0 {
		srv.shutdownTimeout = opts.ShutdowmTimeout
	} else {
		srv.shutdownTimeout = 60 * time.Second
	}

	return srv
}

type Server struct {
	srv *http.Server
	mux *http.ServeMux

	name            string
	shutdownTimeout time.Duration
}

func (s *Server) Name() string {
	return s.name
}

func (s *Server) Run() error {
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

func (s *Server) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}
