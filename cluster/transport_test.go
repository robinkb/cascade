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
		transports[i] = NewTransport(peers[i].ID, peers[i].AddrPort)
	}

	return transports
}

func TestTransportPeerManagement(t *testing.T) {
	peers := newTestPeers(3)
	transports := newTestTransports(peers)

	for _, transport := range transports {
		err := transport.Listen()
		RequireNoError(t, err)
	}

	for _, transport := range transports {
		for _, peer := range peers {
			if peer.ID != transport.ID() {
				err := transport.Add(peer)
				RequireNoError(t, err)
			}
		}
	}

	t.Run("Listing peers returns all peers", func(t *testing.T) {
		gotPeers := transports[0].Peers()
		AssertEqual(t, len(gotPeers), len(peers)-1)
	})

	t.Run("Adding a peer that already exists errors out", func(t *testing.T) {
		err := transports[0].Add(peers[1])
		AssertErrorIs(t, err, ErrDuplicatePeer)
	})

	t.Run("A removed peer is not retrievable", func(t *testing.T) {
		peers := newTestPeers(2)
		transports := newTestTransports(peers)

		for _, transport := range transports {
			err := transport.Listen()
			RequireNoError(t, err)
		}

		for _, transport := range transports {
			for _, peer := range peers {
				if peer.ID != transport.ID() {
					err := transport.Add(peer)
					RequireNoError(t, err)
				}
			}
		}

		err := transports[0].Remove(peers[1].ID)
		AssertNoError(t, err)

		_, err = transports[0].Peer(peers[1].ID)
		AssertErrorIs(t, err, ErrPeerNotFound)
	})
}

func TestTransportSingleTransmission(t *testing.T) {
	receiverP := Peer{ID: 1, AddrPort: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	receiver := NewTransport(receiverP.ID, receiverP.AddrPort)
	err := receiver.Listen()
	RequireNoError(t, err)

	senderP := Peer{ID: 2, AddrPort: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	sender := NewTransport(senderP.ID, senderP.AddrPort)
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
	receiver := NewTransport(receiverP.ID, receiverP.AddrPort)
	err := receiver.Listen()
	RequireNoError(t, err)

	senderP := Peer{ID: 2, AddrPort: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	sender := NewTransport(senderP.ID, senderP.AddrPort)
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
