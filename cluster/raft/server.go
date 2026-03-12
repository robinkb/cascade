package raft

import (
	"io"
	"net/http"

	"github.com/golang/protobuf/proto"
	godigest "github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
	"go.etcd.io/raft/v3/raftpb"
)

func NewDirtyServer(node Node, blobs store.BlobReader) *server {
	server := NewServer(node)
	server.blobs = blobs
	return server
}

func NewServer(node Node) *server {
	s := new(server)
	s.node = node

	router := http.NewServeMux()
	router.Handle("/message", http.HandlerFunc(s.messageHandler))
	router.Handle("/blobs/{digest}", http.HandlerFunc(s.blobHandler))

	s.Handler = router

	return s
}

type server struct {
	http.Handler
	node  Node
	blobs store.BlobReader
}

func (s *server) messageHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.postMessageHandler(w, r)
	default:
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *server) postMessageHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Properly handle errors
	data, _ := io.ReadAll(r.Body)
	var message raftpb.Message
	_ = proto.Unmarshal(data, &message)
	_ = s.node.Receive(&message)
	w.WriteHeader(http.StatusOK)
}

func (s *server) blobHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.getBlobHandler(w, r)
	default:
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *server) getBlobHandler(w http.ResponseWriter, r *http.Request) {
	digest := r.PathValue("digest")

	id, err := godigest.Parse(digest)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	rd, err := s.blobs.BlobReader(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	_, err = io.Copy(w, rd)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
