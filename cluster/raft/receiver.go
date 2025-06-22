package raft

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/netip"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	Mesh interface {
		SetPeer(id uint64, addr netip.AddrPort)
		DeletePeer(id uint64)
		SendMessage(id uint64, m raftpb.Message)
		GetSnapshot(id uint64)
	}

	// Receiver receives Raft messages from the network.
	Receiver interface {
		Receive(msg *raftpb.Message) error
	}
)

func NewServer(node Node) *server {
	s := new(server)
	s.node = node

	router := http.NewServeMux()
	router.Handle("/message", http.HandlerFunc(s.messageHandler))
	// router.Handle("/snapshot", http.HandlerFunc(s.snapshotHandler))

	s.Handler = router

	return s
}

type server struct {
	http.Handler
	node Node
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

// func (s *server) snapshotHandler(w http.ResponseWriter, r *http.Request) {
// 	switch r.Method {
// 	case http.MethodGet:
// 		s.getSnapshotHandler(w, r)
// 	default:
// 		w.Header().Set("Allow", http.MethodGet)
// 		w.WriteHeader(http.StatusMethodNotAllowed)
// 	}
// }

// func (s *server) getSnapshotHandler(w http.ResponseWriter, _ *http.Request) {
// 	// TODO: Handle errors
// 	snapshot, _ := s.service.GetSnapshot()
// 	w.WriteHeader(http.StatusOK)
// 	_, _ = io.Copy(w, snapshot)
// }

func NewClient(baseUrl string) *Client {
	return &Client{
		client:  new(http.Client),
		baseUrl: baseUrl,
	}
}

type Client struct {
	client  *http.Client
	baseUrl string
}

func (c *Client) SendMessage(m *raftpb.Message) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}

	resp, err := c.do(http.MethodPost, "/message", nil, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("unexpected error")
	}
	return nil
}

func (c *Client) GetSnapshot() (io.Reader, error) {
	resp, err := c.do(http.MethodGet, "/snapshot", nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("unexpected error")
	}

	return resp.Body, nil
}

func (c *Client) do(method string, path string, headers http.Header, body io.Reader) (*http.Response, error) {
	url := c.baseUrl + path
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header = headers

	return c.client.Do(req)
}
