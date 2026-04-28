package raft

import (
	"context"
	"io"
	"net/http"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
)

func (n *node) Handler() http.Handler {
	h := new(Handler)

	h.node = n

	mux := http.NewServeMux()
	mux.Handle("/message", http.HandlerFunc(h.messageHandler))

	h.Handler = mux

	return h
}

type Handler struct {
	http.Handler
	node Node
}

func (h *Handler) messageHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.postMessageHandler(w, r)
	default:
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) postMessageHandler(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var message raftpb.Message
	if err := proto.Unmarshal(data, &message); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := h.node.Receive(message); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (n *node) Receive(msg raftpb.Message) error {
	return n.raft.Step(context.TODO(), msg)
}
