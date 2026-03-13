package api

import (
	"io"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/robinkb/cascade/cluster/raft"
	"go.etcd.io/raft/v3/raftpb"
)

func New(node raft.Node) *Handler {
	h := new(Handler)

	h.node = node

	mux := http.NewServeMux()
	mux.Handle("/message", http.HandlerFunc(h.messageHandler))

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
	// TODO: Properly handle errors
	data, _ := io.ReadAll(r.Body)
	var message raftpb.Message
	_ = proto.Unmarshal(data, &message)
	_ = h.node.Receive(&message)
	w.WriteHeader(http.StatusOK)
}
