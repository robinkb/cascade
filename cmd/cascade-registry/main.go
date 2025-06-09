package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
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
		addr, err := net.ResolveTCPAddr("tcp", raftHostPort)
		if err != nil {
			log.Fatal(err)
		}

		hosts := strings.Split(raftPeers, ",")
		peers := make([]raft.Peer, len(hosts))
		for i := range hosts {
			parts := strings.Split(hosts[i], ":")
			id, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			port, err := strconv.ParseInt(parts[2], 10, 32)
			if err != nil {
				log.Fatal(err)
			}
			peers[i] = raft.Peer{
				ID: id,
				Addr: &net.TCPAddr{
					IP:   net.ParseIP(parts[1]),
					Port: int(port),
				},
			}
		}

		node := raft.NewNode(uint64(raftId), addr, peers)
		metadata = cluster.NewMetadataStore(node, metadata)
		blobs = cluster.NewBlobStore(node, blobs)
		node.Start()
	}

	service := cascade.NewRegistryService(metadata, blobs)
	server := server.New(service)

	addr := net.TCPAddr{
		Port: port,
	}
	log.Fatal(http.ListenAndServe(addr.String(), server))
}
