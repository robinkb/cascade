package cluster

import (
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade/testing"
)

func TestClients(t *testing.T) {
	t.Run("adds and retrieves a client", func(t *testing.T) {
		clients := NewClients[testClient]()

		want := newTestClient()
		err := clients.Add(want.id, want)
		AssertNoError(t, err)

		got, err := clients.Get(want.id)
		AssertNoError(t, err)
		AssertEqual(t, got.id, want.id)
	})

	t.Run("adding the same client twice returns error", func(t *testing.T) {
		clients := NewClients[testClient]()

		want := ErrDuplicateClient

		c := newTestClient()
		got := clients.Add(c.id, c)
		AssertNoError(t, got)
		got = clients.Add(c.id, c)
		AssertErrorIs(t, got, want)
	})

	t.Run("getting non-existent client returns error", func(t *testing.T) {
		clients := NewClients[testClient]()

		want := ErrClientNotFound
		_, got := clients.Get(rand.Uint64())
		AssertErrorIs(t, got, want)
	})

	t.Run("deleted client is not retrievable", func(t *testing.T) {
		clients := NewClients[testClient]()

		want := ErrClientNotFound

		c := newTestClient()
		err := clients.Add(c.id, c)
		AssertNoError(t, err)
		_, err = clients.Get(c.id)
		AssertNoError(t, err)

		clients.Remove(c.id)
		_, got := clients.Get(c.id)
		AssertErrorIs(t, got, want)
	})
}

type testClient struct {
	id uint64
}

func newTestClient() *testClient {
	return &testClient{
		id: rand.Uint64(),
	}
}
