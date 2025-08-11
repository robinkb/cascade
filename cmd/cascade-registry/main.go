package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/cluster/raft"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store/boltdb"
	"github.com/robinkb/cascade-registry/store/cluster"
	"github.com/robinkb/cascade-registry/store/fs"
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

	metadata := boltdb.NewMetadataStore(path)
	blobs := fs.NewBlobStore(path)

	if raftId != 0 {
		addr := netip.MustParseAddrPort(raftHostPort)

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

		node := raft.NewNode(uint64(raftId), addr, peers, filepath.Join(path, "raft"), metadata)
		metadata = cluster.NewMetadataStore(node, metadata)
		blobs = cluster.NewBlobStore(node, blobs)
		node.Start()
		defer func() {
			if err := node.Stop(); err != nil {
				log.Println("error while stopping Raft node:", err)
			}
		}()
	}

	service := cascade.NewRegistryService(metadata, blobs)
	server := server.New(service)

	addr := net.TCPAddr{
		Port: port,
	}
	log.Fatal(http.ListenAndServe(addr.String(), server))
}
