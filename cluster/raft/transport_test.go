package raft

import (
	"bytes"
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

func RandomMessages(messages chan<- raftpb.Message, n int) {
	for i := range n {
		log.Println("sending", i)
		messages <- raftpb.Message{
			Type:   raftpb.MsgApp,
			Term:   rand.Uint64(),
			Index:  rand.Uint64(),
			Commit: rand.Uint64(),
		}
	}
}

func TestTransport(t *testing.T) {
	port := RandomPort()
	addr := &net.TCPAddr{
		Port: port,
	}

	messages := make(chan raftpb.Message)

	go receiver(addr)
	time.Sleep(100 * time.Millisecond)
	go sender(addr, messages)

	RandomMessages(messages, 50)
}

func receiver(addr *net.TCPAddr) {
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

func sender(addr *net.TCPAddr, messages <-chan raftpb.Message) {
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
