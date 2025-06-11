package cluster

import (
	"errors"
	"log"
	"maps"
	"net"
	"net/netip"
)

type (
	Transport interface {
		Peers() []Peer
		Peer(id uint64) (Peer, error)
		Add(p Peer) error
		Remove(id uint64) error

		Listen() error
		Close() error
		Send(id uint64, data []byte) error
		Receive() <-chan []byte
	}

	Peer struct {
		ID   uint64
		Addr netip.AddrPort
	}
)

var (
	ErrDuplicatePeer = errors.New("duplicate peer")
	ErrPeerNotFound  = errors.New("peer not found")
)

func NewTransport(addr netip.AddrPort) Transport {
	return &transport{
		addr:    addr,
		peers:   make(map[uint64]Peer),
		conns:   make(map[uint64]net.Conn),
		receive: make(chan []byte),
	}
}

type transport struct {
	addr     netip.AddrPort
	peers    map[uint64]Peer
	listener net.Listener
	conns    map[uint64]net.Conn
	receive  chan []byte
}

func (t *transport) Peers() []Peer {
	peers := make([]Peer, 0)
	for peer := range maps.Values(t.peers) {
		peers = append(peers, peer)
	}
	return peers
}

func (t *transport) Peer(id uint64) (Peer, error) {
	peer, ok := t.peers[id]
	if !ok {
		return Peer{}, ErrPeerNotFound
	}
	return peer, nil
}

func (t *transport) Add(p Peer) error {
	if _, ok := t.peers[p.ID]; ok {
		return ErrDuplicatePeer
	}
	t.peers[p.ID] = p
	return nil
}

func (t *transport) Remove(id uint64) error {
	delete(t.peers, id)
	return nil
}

func (t *transport) Listen() error {
	l, err := net.Listen("tcp", t.addr.String())
	t.listener = l
	go t.listen()
	return err
}

func (t *transport) listen() {
	for {
		// Wait for a connection.
		conn, err := t.listener.Accept()
		if err != nil {
			panic(err)
		}

		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			defer c.Close()

			decoder := NewDecoder()
			for {
				data, err := decoder.Decode(conn)
				if err != nil {
					log.Panicln("failed to decode message:", err)
				}
				t.receive <- data
			}
		}(conn)
	}
}

func (t *transport) Close() error {
	return t.listener.Close()
}

func (t *transport) Send(id uint64, data []byte) error {
	conn, err := net.Dial("tcp", t.peers[id].Addr.String())
	if err != nil {
		return err
	}
	defer conn.Close()

	encoder := NewEncoder()
	return encoder.Encode(conn, data)
}

func (t *transport) Receive() <-chan []byte {
	return t.receive
}
