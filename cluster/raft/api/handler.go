package api

import (
	"io"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/robinkb/cascade/cluster/raft"
	"go.etcd.io/raft/v3/raftpb"
)

func New(node raft.Node) *Handler {
	h := new(Handler)

	h.node = node

	mux := http.NewServeMux()
	mux.Handle("/message", http.HandlerFunc(h.messageHandler))

	h.Handler = mux

	return h
}

type Handler struct {
	http.Handler
	node raft.Node
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

	if err := h.node.Receive(&message); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
