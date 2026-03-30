package cluster_test

import (
	"math/rand/v2"
	"testing"

	"github.com/robinkb/cascade/cluster"
	. "github.com/robinkb/cascade/testing"
)

func TestClients(t *testing.T) {
	t.Run("adds and retrieves a client", func(t *testing.T) {
		clients := cluster.NewClients[testClient]()

		want := newTestClient()
		err := clients.Add(want.Peer, want)
		AssertNoError(t, err)

		got, err := clients.Get(want.ID)
		AssertNoError(t, err)
		AssertEqual(t, got.ID, want.ID)
	})

	t.Run("adding the same client twice returns error", func(t *testing.T) {
		clients := cluster.NewClients[testClient]()

		want := cluster.ErrDuplicateClient

		c := newTestClient()
		got := clients.Add(c.Peer, c)
		AssertNoError(t, got)
		got = clients.Add(c.Peer, c)
		AssertErrorIs(t, got, want)
	})

	t.Run("getting non-existent client returns error", func(t *testing.T) {
		clients := cluster.NewClients[testClient]()

		want := cluster.ErrClientNotFound
		_, got := clients.Get(rand.Uint64())
		AssertErrorIs(t, got, want)
	})

	t.Run("deleted client is not retrievable", func(t *testing.T) {
		clients := cluster.NewClients[testClient]()

		want := cluster.ErrClientNotFound

		c := newTestClient()
		err := clients.Add(c.Peer, c)
		AssertNoError(t, err)
		_, err = clients.Get(c.ID)
		AssertNoError(t, err)

		clients.Remove(c.ID)
		_, got := clients.Get(c.ID)
		AssertErrorIs(t, got, want)
	})
}

type testClient struct {
	cluster.Peer
}

func newTestClient() *testClient {
	return &testClient{
		Peer: cluster.Peer{
			ID: rand.Uint64(),
		},
	}
}
