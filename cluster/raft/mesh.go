package raft

import (
	"log"
	"net/http"
	"net/netip"

	"github.com/robinkb/cascade/registry/store"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	Mesh interface {
		Start()
	}

	// Receiver receives Raft messages from the network.
	Receiver interface {
		Receive(msg *raftpb.Message) error
	}

	Peer struct {
		ID       uint64
		AddrPort netip.AddrPort
	}
)

func NewDirtyMesh(node Node, addr netip.AddrPort, blobs store.BlobReader) Mesh {
	return &mesh{
		addr:   addr,
		server: NewDirtyServer(node, blobs),
	}
}

func NewMesh(node Node, addr netip.AddrPort) Mesh {
	m := &mesh{
		addr:   addr,
		server: NewServer(node),
	}

	return m
}

type mesh struct {
	addr   netip.AddrPort
	server *server
}

func (m *mesh) Start() {
	go func() {
		if err := http.ListenAndServe(m.addr.String(), m.server); err != nil {
			log.Println("error closing raft http server:", err)
		}
	}()
}

func NewDirtyServer(node Node, blobs store.BlobReader) *server {
	server := NewServer(node)
	server.blobs = blobs
	return server
}
