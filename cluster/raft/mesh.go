package raft

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net/http"
	"net/netip"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	Mesh interface {
		Start()
		SetPeer(id uint64, addr netip.AddrPort)
		DeletePeer(id uint64)
		SendMessage(id uint64, msg *raftpb.Message) error
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

func NewMesh(node Node, addr netip.AddrPort) Mesh {
	m := &mesh{
		addr:    addr,
		server:  NewServer(node),
		clients: make(map[uint64]*Client),
	}

	return m
}

type mesh struct {
	addr    netip.AddrPort
	server  *server
	clients map[uint64]*Client
}

func (m *mesh) Start() {
	go func() {
		if err := http.ListenAndServe(m.addr.String(), m.server); err != nil {
			log.Println("error closing raft http server:", err)
		}
	}()
}

func (m *mesh) SetPeer(id uint64, addr netip.AddrPort) {
	m.clients[id] = NewClient("http://" + addr.String())
}

func (m *mesh) DeletePeer(id uint64) {
	delete(m.clients, id)
}

func (m *mesh) SendMessage(id uint64, msg *raftpb.Message) error {
	return m.clients[id].SendMessage(msg)
}

func NewServer(node Node) *server {
	s := new(server)
	s.node = node

	router := http.NewServeMux()
	router.Handle("/message", http.HandlerFunc(s.messageHandler))

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
