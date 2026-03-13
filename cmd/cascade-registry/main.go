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

	"github.com/robinkb/cascade/cluster/raft"
	"github.com/robinkb/cascade/cluster/raft/api"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"github.com/robinkb/cascade/process"
	"github.com/robinkb/cascade/registry"
	v2 "github.com/robinkb/cascade/registry/api/v2"
	"github.com/robinkb/cascade/registry/store/boltdb"
	"github.com/robinkb/cascade/registry/store/cluster"
	"github.com/robinkb/cascade/registry/store/fs"
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

	metadata := boltdb.NewMetadataStore(path)
	blobs := fs.NewBlobStore(path)

	if raftId != 0 {
		addr := netip.MustParseAddrPort(raftHostPort)
		srv := server.NewServer(server.ServerOptions{
			Name: "cluster-server",
			Addr: addr,
		})

		hosts := strings.Split(raftPeers, ",")
		peers := make([]raft.Peer, len(hosts))
		for i := range hosts {
			parts := strings.Split(hosts[i], ":")
			id, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			host := strings.Join(parts[1:3], ":")
			peers[i] = raft.Peer{
				ID:       id,
				AddrPort: netip.MustParseAddrPort(host),
			}
		}

		db, err := qwal.Open(filepath.Join(path, "raft"), nil)
		if err != nil {
			log.Fatal(err)
		}
		store, err := raft.NewDiskStorage(db, metadata)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := store.Close(); err != nil {
				log.Println("error while closing raft storage:", err)
			}
		}()

		node := raft.NewNode(uint64(raftId), addr, store, metadata)
		metadata = cluster.NewMetadataStore(node, metadata)
		blobs = cluster.NewBlobStore(node, blobs)
		node.Bootstrap(peers...)

		srv.Handle("/", api.New(node))
		mgr.Register(srv)
		mgr.Register(node)
	}

	service := registry.NewService(metadata, blobs)
	api := v2.New(service)

	srv := server.NewServer(server.ServerOptions{
		Name: "oci-api",
		Addr: netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", port)),
	})

	srv.Handle("/", api)

	mgr.Register(srv)

	if err := mgr.Run(); err != nil {
		log.Fatal(err)
	}
}
