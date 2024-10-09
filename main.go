package main

import (
	"log"
	"net/http"
)

func main() {
	store := NewInMemoryStore()
	service := NewRegistryService(store)
	server := NewRegistryServer(service)
	log.Fatal(http.ListenAndServe(":5000", server))
}
