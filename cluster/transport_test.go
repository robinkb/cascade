package cluster

import (
	"fmt"
	"math/rand/v2"
	"net/netip"
	"sync"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func newTestPeers(n int) []Peer {
	peers := make([]Peer, n)
	for i := range n {
		peers[i] = Peer{
			ID: rand.Uint64(),
			AddrPort: netip.MustParseAddrPort(
				fmt.Sprintf("127.0.0.1:%d", RandomPort()),
			),
		}
	}
	return peers
}

func newTestTransports(peers []Peer) []Transport {
	transports := make([]Transport, len(peers))
	for i := range transports {
		transports[i] = NewTransport(peers[i].AddrPort)
		for j := range peers {
			if i != j {
				transports[i].Add(peers[j])
			}
		}
	}

	return transports
}

func TestTransportPeerManagement(t *testing.T) {
	t.Run("Listing peers returns all peers", func(t *testing.T) {
		peers := newTestPeers(4)
		transport := NewTransport(peers[0].AddrPort)

		for _, peer := range peers {
			err := transport.Add(peer)
			RequireNoError(t, err)
		}

		gotPeers := transport.Peers()
		AssertEqual(t, len(gotPeers), len(peers))
	})

	t.Run("Adding a peer that already exists errors out", func(t *testing.T) {
		peer := newTestPeers(1)[0]
		transport := NewTransport(peer.AddrPort)

		err := transport.Add(peer)
		RequireNoError(t, err)
		_, err = transport.Peer(peer.ID)
		RequireNoError(t, err)

		err = transport.Add(peer)
		AssertErrorIs(t, err, ErrDuplicatePeer)
	})

	t.Run("A removed peer is not retrievable", func(t *testing.T) {
		peer := newTestPeers(1)[0]
		transport := NewTransport(peer.AddrPort)

		err := transport.Add(peer)
		RequireNoError(t, err)
		gotPeer, err := transport.Peer(peer.ID)
		AssertNoError(t, err)
		AssertStructsEqual(t, gotPeer, peer)

		err = transport.Remove(peer.ID)
		AssertNoError(t, err)

		_, err = transport.Peer(peer.ID)
		AssertErrorIs(t, err, ErrPeerNotFound)
	})
}

func TestTransportSingleTransmission(t *testing.T) {
	receiverP := Peer{ID: 1, AddrPort: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	receiver := NewTransport(receiverP.AddrPort)
	err := receiver.Listen()
	RequireNoError(t, err)

	senderP := Peer{ID: 2, AddrPort: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	sender := NewTransport(senderP.AddrPort)
	sender.Add(receiverP)

	var got []byte
	var want = RandomContents(128)
	err = sender.Send(receiverP.ID, want)
	AssertNoError(t, err)

	got = <-receiver.Receive()
	AssertSlicesEqual(t, got, want)
}

func TestTransportMultipleTransmissions(t *testing.T) {
	n := 1000
	receiverP := Peer{ID: 1, AddrPort: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	receiver := NewTransport(receiverP.AddrPort)
	err := receiver.Listen()
	RequireNoError(t, err)

	senderP := Peer{ID: 2, AddrPort: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	sender := NewTransport(senderP.AddrPort)
	sender.Add(receiverP)

	want := make([][]byte, n)
	for i := range n {
		want[i] = RandomContents(rand.Int64N(128 << 10))
	}

	var wg sync.WaitGroup
	wg.Add(n)
	go func() {
		for i := range n {
			got := <-receiver.Receive()
			AssertSlicesEqual(t, got, want[i])
			wg.Done()
		}
	}()

	for i := range n {
		err := sender.Send(receiverP.ID, want[i])
		RequireNoError(t, err)
	}

	wg.Wait()
}
