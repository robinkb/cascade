package cluster

import (
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestListenerReceiver(t *testing.T) {
	r := NewReceiver(RandomAddrPort())
	s := NewSender(r.AddrPort())

	err := r.Listen()
	RequireNoError(t, err)

	s.Dial()

	want := RandomContents(128)
	err = s.Send(want)
	RequireNoError(t, err)

	got := <-r.Receive()
	AssertSlicesEqual(t, got, want)

	s.Close()
	r.Close()
}

/**
What I need:

1. A Listener
	a. that restarts when it encounters an error
	b. until a "stop" signal is given
	c. gracefully shuts down
	d. with exponential backoff
2. A Dialer
	a. Same
*/
