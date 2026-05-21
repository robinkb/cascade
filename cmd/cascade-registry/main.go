package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	kongyaml "github.com/alecthomas/kong-yaml"
	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/raft"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"github.com/robinkb/cascade/pkg/process"
	"github.com/robinkb/cascade/pkg/server"
	"github.com/robinkb/cascade/registry"
	registryapi "github.com/robinkb/cascade/registry/api/v2"
	"github.com/robinkb/cascade/registry/store"
	storeapi "github.com/robinkb/cascade/registry/store/api"
	"github.com/robinkb/cascade/registry/store/driver/boltdb"
	clusterstore "github.com/robinkb/cascade/registry/store/driver/cluster"
	"github.com/robinkb/cascade/registry/store/driver/fs"

	// Embed tzdata to run from scratch.
	_ "time/tzdata"

	// Embed CA certificates to run from scratch.
	_ "golang.org/x/crypto/x509roots/fallback"
)

var cli struct {
	Config kong.ConfigFlag `help:"File to load configuration from."`

	Port         int      `help:"Port of the Registry HTTP server."`
	RaftID       uint64   `help:"ID of this Raft node."`
	RaftHostPort string   `help:"Host of this Raft node."`
	RaftPeers    []string `help:"Comma-separated list of Raft peers."`
}

func main() {
	kong.Parse(&cli,
		kong.DefaultEnvars("cascade"),
		kong.Configuration(kongyaml.Loader, "/etc/cascade/config.yaml"),
	)

	path, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %s", err)
	}

	mgr := process.NewManager()

	metadata, err := boltdb.NewMetadataStore(path)
	if err != nil {
		log.Fatalf("failed to create metadata store backed by boltdb: %s", err)
	}
	blobs := fs.NewBlobStore(path)

	if cli.RaftID != 0 {
		srv := server.New(server.Options{
			Name: "cluster-server",
			Addr: cli.RaftHostPort,
		})

		peers := make([]cluster.Peer, len(cli.RaftPeers))
		for i := range cli.RaftPeers {
			parts := strings.Split(cli.RaftPeers[i], ":")
			id, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			host := strings.Join(parts[1:3], ":")
			peers[i] = cluster.Peer{
				ID:   id,
				Addr: host,
			}
		}

		db, err := qwal.Open(filepath.Join(path, "raft"), nil)
		if err != nil {
			log.Fatal(err)
		}
		storage, err := raft.NewDiskStorage(db, metadata)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := storage.Close(); err != nil {
				log.Println("error while closing raft storage:", err)
			}
		}()
		restorer := store.NewRestorer(metadata, blobs)
		node := raft.NewNode(cli.RaftID, cli.RaftHostPort, storage, restorer)
		// Shit, this is needed because the node has to be running.
		// And the node won't be running until the manager starts.
		// And starting the manager is a blocking call.
		go func() {
			time.Sleep(10 * time.Millisecond)
			node.Bootstrap(peers...)
		}()

		srv.Handle("/cluster/raft/", node.Handler())
		srv.Handle("/store/", storeapi.New(blobs))
		mgr.Register(srv)
		mgr.Register(node)

		metadata = clusterstore.NewMetadataStore(node, metadata)
		blobs = clusterstore.NewBlobStore(node, blobs)
	}

	srv := server.New(server.Options{
		Name:          "oci-api",
		Addr:          fmt.Sprintf("0.0.0.0:%d", cli.Port),
		LoggerEnabled: true,
	})

	service := registry.New(metadata, blobs)
	srv.Handle("/", registryapi.New(service))

	mgr.Register(srv)

	if err := mgr.Run(); err != nil {
		log.Fatal(err)
	}
}
