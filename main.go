package main

import (
	"log"
	"net/http"
)

func main() {
	store := NewInMemoryStore()
	service := NewRegistryService(store)
	server := NewRegistryServer(service)
	log.Fatal(http.ListenAndServe(":5000", logger(server)))
}

func logger(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		log.Printf("%s %s %v\n", r.Method, r.URL.Path, r.Header)
	})
}
