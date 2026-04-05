package main

import (
	"flag"
	"fmt"
	"log"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/raft"
	raftapi "github.com/robinkb/cascade/cluster/raft/api"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"github.com/robinkb/cascade/process"
	"github.com/robinkb/cascade/registry"
	registryapi "github.com/robinkb/cascade/registry/api/v2"
	"github.com/robinkb/cascade/registry/store"
	storeapi "github.com/robinkb/cascade/registry/store/api"
	"github.com/robinkb/cascade/registry/store/driver/boltdb"
	clusterstore "github.com/robinkb/cascade/registry/store/driver/cluster"
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
	flag.IntVar(&raftId, "raft-id", 0, "internal id of raft node")
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
		addr := netip.MustParseAddrPort(raftHostPort)
		srv := server.New(server.Options{
			Name: "cluster-server",
			Addr: addr,
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
				ID:       id,
				AddrPort: netip.MustParseAddrPort(host),
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
		node := raft.NewNode(uint64(raftId), addr, storage, restorer)
		node.Bootstrap(peers...)

		srv.Handle("/cluster/raft/", raftapi.New(node))
		srv.Handle("/store/", storeapi.New(blobs))
		mgr.Register(srv)
		mgr.Register(node)

		metadata = clusterstore.NewMetadataStore(node, metadata)
		blobs = clusterstore.NewBlobStore(node, blobs)
	}

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
