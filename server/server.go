package server

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/netip"
	"time"
)

type ServerOptions struct {
	Name            string
	Addr            netip.AddrPort
	ShutdowmTimeout time.Duration
}

func NewServer(opts ServerOptions) *Server {
	mux := http.NewServeMux()
	srv := &Server{
		srv: &http.Server{
			// TODO: Can't just enable this globally
			// because every Raft message would produce a log line.
			// Handler: logger(mux),
			Handler: mux,
		},
		mux: mux,
	}

	srv.name = opts.Name
	srv.srv.Addr = opts.Addr.String()

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
	err := s.srv.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

func (s *Server) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

func logger(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lw := NewResponseWriter(w)
		handler.ServeHTTP(lw, r)
		log.Printf("%-8s %s %d %s", r.Method, r.URL.Path, lw.Code(), r.Header.Get("User-Agent"))
	})
}
