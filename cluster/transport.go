package cluster

import (
	"errors"
	"log"
	"maps"
	"net"
	"net/netip"
	"sync"
	"time"
)

type (
	Transport interface {
		ID() uint64
		Peers() []Peer
		Peer(id uint64) (Peer, error)
		Add(p Peer) error
		Remove(id uint64) error

		Listen() error
		Close() error
		Send(id uint64, data []byte) error
		Receive() <-chan *Message
	}

	Peer struct {
		ID       uint64
		AddrPort netip.AddrPort
	}

	Message struct {
		Data  []byte
		Error error
	}
)

var (
	ErrDuplicatePeer = errors.New("duplicate peer")
	ErrPeerNotFound  = errors.New("peer not found")
)

func NewTransport(id uint64, addr netip.AddrPort) Transport {
	return &transport{
		id:    id,
		addr:  addr,
		peers: make(map[uint64]Peer),

		receive: make(chan *Message),

		send:  make(map[uint64]chan []byte),
		errs:  make(map[uint64]chan error),
		ready: make(map[uint64]chan struct{}),
	}
}

type transport struct {
	mu sync.RWMutex

	id    uint64
	addr  netip.AddrPort
	peers map[uint64]Peer

	listener net.Listener
	receive  chan *Message

	send  map[uint64]chan []byte
	errs  map[uint64]chan error
	ready map[uint64]chan struct{}
}

func (t *transport) ID() uint64 {
	return t.id
}

func (t *transport) Peers() []Peer {
	peers := make([]Peer, 0)
	for peer := range maps.Values(t.peers) {
		peers = append(peers, peer)
	}
	return peers
}

func (t *transport) Peer(id uint64) (Peer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	peer, ok := t.peers[id]
	if !ok {
		return Peer{}, ErrPeerNotFound
	}
	return peer, nil
}

func (t *transport) Add(p Peer) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.peers[p.ID]; ok {
		return ErrDuplicatePeer
	}

	t.peers[p.ID] = p
	t.send[p.ID] = make(chan []byte)
	t.errs[p.ID] = make(chan error)

	t.ready[p.ID] = make(chan struct{})
	go t.dial(p.ID)
	<-t.ready[p.ID]

	return nil
}

func (t *transport) Remove(id uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

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
			defer func() {
				if err := c.Close(); err != nil {
					log.Println("error while closing connection:", err)
				}
			}()

			d := NewVarIntDecoder()
			for {
				data, err := d.VarIntDecode(conn)
				if err != nil {
					log.Panicln("failed to decode message:", err)
				}
				t.receive <- &Message{
					Data:  data,
					Error: err,
				}
			}
		}(conn)
	}
}

func (t *transport) Close() error {
	return t.listener.Close()
}

func (t *transport) Send(id uint64, data []byte) error {
	t.send[id] <- data
	return <-t.errs[id]
}

func (t *transport) dial(id uint64) {
	for {
		conn, err := net.Dial("tcp", t.peers[id].AddrPort.String())
		if err != nil {
			log.Printf("failed to dial peer %d: %s", id, err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// TODO: Manage closing this connection when shutting down
		// the transport.
		// defer conn.Close()
		close(t.ready[id])

		e := NewVarIntEncoder()
		for data := range t.send[id] {
			t.errs[id] <- e.VarIntEncode(conn, data)
		}
	}
}

func (t *transport) Receive() <-chan *Message {
	return t.receive
}
