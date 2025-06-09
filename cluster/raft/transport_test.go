package raft

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	. "github.com/robinkb/cascade-registry/testing"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func BenchmarkTransportPerMessage(b *testing.B) {
	port := RandomPort()
	addr := &net.TCPAddr{
		Port: port,
	}

	messages := make(chan raftpb.Message)

	go receiverConnPerMessage(addr)
	time.Sleep(time.Millisecond)
	go senderConnPerMessage(addr, messages)

	for b.Loop() {
		RandomMessage(messages)
	}
}

func BenchmarkTransportReuse(b *testing.B) {
	port := RandomPort()
	addr := &net.TCPAddr{
		Port: port,
	}

	messages := make(chan raftpb.Message)

	go receiverReuse(addr)
	time.Sleep(time.Millisecond)
	go senderReuse(addr, messages)

	for b.Loop() {
		RandomMessage(messages)
	}
}

func TestTransportReuse(t *testing.T) {
	port := RandomPort()
	addr := &net.TCPAddr{
		Port: port,
	}

	messages := make(chan raftpb.Message)

	go receiverReuse(addr)
	time.Sleep(time.Millisecond)
	go senderReuse(addr, messages)

	RandomMessages(messages, 100000)
}

func RandomMessages(messages chan<- raftpb.Message, n int) {
	for range n {
		RandomMessage(messages)
	}
}

func RandomMessage(messages chan<- raftpb.Message) {
	messages <- raftpb.Message{
		Type:   raftpb.MsgApp,
		Term:   rand.Uint64(),
		Index:  rand.Uint64(),
		Commit: rand.Uint64(),
	}
}

func receiverReuse(addr *net.TCPAddr) {
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			log.Panic(err)
		}
	}()

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Panic(err)
		}

		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			defer func() {
				if err := c.Close(); err != nil {
					log.Panic(err)
				}
			}()

			var i int
			r := bufio.NewReader(c)
			for {
				i++
				varint := make([]byte, 4)
				_, err := io.ReadFull(r, varint)
				if err != nil {
					log.Panic(err)
				}

				buf := make([]byte, binary.LittleEndian.Uint32(varint))
				_, err = io.ReadFull(r, buf)
				if err != nil {
					log.Panic(err)
				}

				var msg raftpb.Message
				if err := msg.Unmarshal(buf); err != nil {
					log.Panic(err)
				}
			}
		}(conn)
	}
}

func senderReuse(addr *net.TCPAddr, messages <-chan raftpb.Message) {
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Panic(err)
	}

	for message := range messages {
		data, err := proto.Marshal(&message)
		if err != nil {
			log.Panic(err)
		}

		varint := make([]byte, 4)
		binary.LittleEndian.PutUint32(varint, uint32(len(data)))
		data = append(varint, data...)

		if _, err := io.Copy(conn, bytes.NewBuffer(data)); err != nil {
			log.Panic(err)
		}
	}
}

func receiverConnPerMessage(addr *net.TCPAddr) {
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			log.Panic(err)
		}
	}()

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Panic(err)
		}

		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			defer func() {
				if err := c.Close(); err != nil {
					log.Panic(err)
				}
			}()

			buf := &bytes.Buffer{}
			if _, err := io.Copy(buf, c); err != nil {
				log.Panic(err)
			}

			var msg raftpb.Message
			if err := msg.Unmarshal(buf.Bytes()); err != nil {
				log.Panic(err)
			}

			log.Println(raft.DescribeMessage(msg, nil))
		}(conn)
	}
}

func senderConnPerMessage(addr *net.TCPAddr, messages <-chan raftpb.Message) {
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Panic(err)
	}

	for message := range messages {
		data, err := proto.Marshal(&message)
		if err != nil {
			log.Panic(err)
		}

		if _, err := io.Copy(conn, bytes.NewBuffer(data)); err != nil {
			log.Panic(err)
		}
	}
}
