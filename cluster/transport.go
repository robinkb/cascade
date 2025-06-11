package cluster

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
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

			varint := make([]byte, 4)
			r := bufio.NewReader(conn)
			for {
				io.ReadFull(r, varint)
				buf := make([]byte, binary.LittleEndian.Uint32(varint))
				io.ReadFull(r, buf)
				t.receive <- buf
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

	varint := make([]byte, 4)
	binary.LittleEndian.PutUint32(varint, uint32(len(data)))
	data = append(varint, data...)
	_, err = io.Copy(conn, bytes.NewBuffer(data))
	return err
}

func (t *transport) Receive() <-chan []byte {
	return t.receive
}

func EncodeWithVarInt(w io.Writer, data []byte) {
	varint := make([]byte, 4)
	binary.LittleEndian.PutUint32(varint, uint32(len(data)))
	data = append(varint, data...)
	io.Copy(w, bytes.NewReader(data))
}

func DecodeWithVarInt(r io.Reader) []byte {
	varint := make([]byte, 4)
	io.ReadFull(r, varint)
	buf := make([]byte, binary.LittleEndian.Uint32(varint))
	io.ReadFull(r, buf)
	return buf
}
