package cluster

import (
	"errors"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"net/netip"
	"time"
)

type (
	Receiver interface {
		AddrPort() netip.AddrPort
		Listen() error
		Receive() <-chan []byte
		Close()
	}

	Sender interface {
		AddrPort() netip.AddrPort
		Dial()
		Send(data []byte) error
		Close()
	}

	Peer struct {
		ID       uint64
		AddrPort netip.AddrPort
	}

	// Message was returned by the Receiver.Receive channel.
	// I might go back to that behavior.
	Message struct {
		Data  []byte
		Error error
	}
)

func NewReceiver(addr netip.AddrPort) Receiver {
	return &receiver{
		addrPort: addr,
		receive:  make(chan []byte),
		done:     make(chan struct{}),
	}
}

type receiver struct {
	addrPort netip.AddrPort
	l        net.Listener
	receive  chan []byte
	done     chan struct{}
}

func (r *receiver) AddrPort() netip.AddrPort {
	return r.addrPort
}

func (r *receiver) Listen() error {
	listener, err := net.Listen("tcp", r.addrPort.String())
	if err != nil {
		return err
	}
	r.l = listener
	go r.listen()
	return nil
}

func (r *receiver) Receive() <-chan []byte {
	return r.receive
}

func (r *receiver) Close() {
	close(r.done)
}

func (r *receiver) listen() {
	for {
		conn, err := r.l.Accept()
		if err != nil {
			log.Printf("unable to accept connections on %s: %s", r.addrPort, err)
			break
		}

		go func(c net.Conn) {
			defer c.Close()

			dec := NewVarIntDecoder()
			for {
				data, err := dec.VarIntDecode(c)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					panic(err)
				}

				r.receive <- data
			}
		}(conn)

		select {
		case <-r.done:
			r.l.Close()
			break
		default:
		}
	}
}

func NewSender(addr netip.AddrPort) Sender {
	return &sender{
		addrPort: addr,
		send:     make(chan []byte),
		err:      make(chan error),
		done:     make(chan struct{}),
	}
}

type sender struct {
	addrPort netip.AddrPort
	send     chan []byte
	err      chan error
	done     chan struct{}
}

func (s *sender) AddrPort() netip.AddrPort {
	return s.addrPort
}

func (s *sender) Dial() {
	go s.dial()
}

func (s *sender) Send(data []byte) error {
	s.send <- data
	return <-s.err
}

func (s *sender) Close() {
	close(s.done)
}

func (s *sender) dial() {
	tries := 0.0
	for {
		conn, err := net.Dial("tcp", s.addrPort.String())
		if err != nil {
			log.Println(err)
			time.Sleep(exponentialBackoff(tries))
			tries++
			continue
		}
		tries = 0
		defer conn.Close()

		enc := NewVarIntEncoder()
	send:
		for {
			select {
			case data := <-s.send:
				err := enc.VarIntEncode(conn, data)
				s.err <- err
				if err != nil {
					log.Println("detected closed connection; reconnecting:", err)
					break send
				}
			case <-s.done:
				return
			}
		}
	}
}

func exponentialBackoff(tries float64) time.Duration {
	if tries >= 5 {
		return 5 * time.Second
	}
	base := rand.Float64() * 50
	backoff := math.Pow(base, tries)
	return time.Duration(backoff) * time.Microsecond
}
