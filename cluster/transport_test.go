package cluster

import (
	"fmt"
	"math/rand/v2"
	"net/netip"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func newTestPeers(n int) []Peer {
	peers := make([]Peer, n)
	for i := range n {
		peers[i] = Peer{
			ID: rand.Uint64(),
			Addr: netip.MustParseAddrPort(
				fmt.Sprintf("127.0.0.1:%d", RandomPort()),
			),
		}
	}
	return peers
}

func newTestTransports(peers []Peer) []Transport {
	transports := make([]Transport, len(peers))
	for i := range transports {
		transports[i] = NewTransport(peers[i].Addr)
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
		transport := NewTransport(peers[0].Addr)

		for _, peer := range peers {
			err := transport.Add(peer)
			RequireNoError(t, err)
		}

		gotPeers := transport.Peers()
		AssertEqual(t, len(gotPeers), len(peers))
	})

	t.Run("Adding a peer that already exists errors out", func(t *testing.T) {
		peer := newTestPeers(1)[0]
		transport := NewTransport(peer.Addr)

		err := transport.Add(peer)
		RequireNoError(t, err)
		_, err = transport.Peer(peer.ID)
		RequireNoError(t, err)

		err = transport.Add(peer)
		AssertErrorIs(t, err, ErrDuplicatePeer)
	})

	t.Run("A removed peer is not retrievable", func(t *testing.T) {
		peer := newTestPeers(1)[0]
		transport := NewTransport(peer.Addr)

		err := transport.Add(peer)
		RequireNoError(t, err)
		gotPeer, err := transport.Peer(peer.ID)
		AssertStructsEqual(t, gotPeer, peer)

		err = transport.Remove(peer.ID)
		AssertNoError(t, err)

		_, err = transport.Peer(peer.ID)
		AssertErrorIs(t, err, ErrPeerNotFound)
	})
}

func TestTransportTransmission(t *testing.T) {
	peer1 := Peer{ID: 1, Addr: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	transport1 := NewTransport(peer1.Addr)
	peer2 := Peer{ID: 2, Addr: netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)}
	transport2 := NewTransport(peer2.Addr)
	transport1.Add(peer2)
	transport2.Add(peer1)

	err := transport1.Listen()
	AssertNoError(t, err)

	var got []byte
	var want []byte = RandomContents(128)
	err = transport2.Send(peer1.ID, want)
	AssertNoError(t, err)

	got = <-transport1.Receive()
	AssertSlicesEqual(t, got, want)
}
