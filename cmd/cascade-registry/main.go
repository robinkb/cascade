package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/raft"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"github.com/robinkb/cascade/process"
	"github.com/robinkb/cascade/registry"
	registryapi "github.com/robinkb/cascade/registry/api/v2"
	storeapi "github.com/robinkb/cascade/registry/store/api"
	"github.com/robinkb/cascade/registry/store/driver/boltdb"
	clusterstore "github.com/robinkb/cascade/registry/store/driver/cluster"
	"github.com/robinkb/cascade/registry/store/driver/fs"
	"github.com/robinkb/cascade/server"

	// Embed tzdata to run from scratch.
	_ "time/tzdata"

	// Embed CA certificates to run from scratch.
	_ "golang.org/x/crypto/x509roots/fallback"
)

var (
	port         int
	raftId       uint64
	raftHostPort string
	raftPeers    string
)

func main() {
	flag.IntVar(&port, "port", 5000, "port of the http server")
	flag.Uint64Var(&raftId, "raft-id", 0, "internal id of raft node")
	flag.StringVar(&raftHostPort, "raft-host", "", "host:port of this raft node")
	flag.StringVar(&raftPeers, "raft-peers", "", "comma-seperated list of raft peers")
	flag.Parse()

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

	if raftId != 0 {
		srv := server.New(server.Options{
			Name: "cluster-server",
			Addr: raftHostPort,
		})

		hosts := strings.Split(raftPeers, ",")
		peers := make([]cluster.Peer, len(hosts))
		for i := range hosts {
			parts := strings.Split(hosts[i], ":")
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
		// TODO: Restore the restorer :)
		// restorer := store.NewRestorer(metadata, blobs)
		// node := raft.NewNode(raftId, raftHostPort, storage, restorer)
		node := raft.NewNode(raftId, raftHostPort, storage)
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
		Addr:          fmt.Sprintf("0.0.0.0:%d", port),
		LoggerEnabled: true,
	})

	service := registry.New(metadata, blobs)
	srv.Handle("/", registryapi.New(service))

	mgr.Register(srv)

	if err := mgr.Run(); err != nil {
		log.Fatal(err)
	}
}
