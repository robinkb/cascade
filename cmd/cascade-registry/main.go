package main

import (
	"flag"
	"fmt"
	"log"
	"net/netip"
	"os"

	"github.com/robinkb/cascade/process"
	"github.com/robinkb/cascade/registry"
	registryapi "github.com/robinkb/cascade/registry/api/v2"
	"github.com/robinkb/cascade/registry/store/driver/boltdb"
	"github.com/robinkb/cascade/registry/store/driver/fs"
	"github.com/robinkb/cascade/server"
)

var (
	port         int
	raftId       int
	raftHostPort string
	raftPeers    string
)

func main() {
	flag.IntVar(&port, "port", 5000, "port of the http server")
	flag.Parse()

	path, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %s", err)
	}

	mgr := process.NewManager()

	metadata, err := boltdb.NewMetadataStore(path)
	blobs := fs.NewBlobStore(path)

	srv := server.New(server.Options{
		Name:          "oci-api",
		Addr:          netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port)),
		LoggerEnabled: true,
	})

	service := registry.NewService(metadata, blobs)
	srv.Handle("/", registryapi.New(service))

	mgr.Register(srv)

	if err := mgr.Run(); err != nil {
		log.Fatal(err)
	}
}
