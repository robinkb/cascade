package main

import (
	"log"
	"net/http"
	"os"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store/boltdb"
	"github.com/robinkb/cascade-registry/store/fs"
)

func main() {
	path, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %s", err)
	}

	metadata := boltdb.NewMetadataStore(path)
	blobs := fs.NewBlobStore(path)
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
