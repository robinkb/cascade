package main

import (
	"log"
	"net/http"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
)

func main() {
	metadata := cascade.NewInMemoryMetadataStore()
	blobs := cascade.NewInMemoryBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)
	server := server.New(service)
	log.Fatal(http.ListenAndServe(":5000", logger(server)))
}

func logger(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		log.Printf("%s %s %v\n", r.Method, r.URL.Path, r.Header)
	})
}
