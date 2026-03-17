package server

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/netip"
	"strings"
	"time"
)

type Options struct {
	Name            string
	Addr            netip.AddrPort
	ShutdowmTimeout time.Duration
	LoggerEnabled   bool
}

func New(opts Options) *Server {
	mux := http.NewServeMux()
	srv := &Server{
		srv: &http.Server{
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

	if opts.LoggerEnabled {
		srv.srv.Handler = logger(mux)
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
	s.mux.Handle(pattern,
		http.StripPrefix(
			strings.TrimSuffix(pattern, "/"),
			handler,
		),
	)
}

func logger(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lw := NewResponseWriter(w)
		handler.ServeHTTP(lw, r)
		log.Printf("%-8s %s %d %s", r.Method, r.URL.Path, lw.Code(), r.Header.Get("User-Agent"))
	})
}
