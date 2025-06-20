package transport

import (
	"io"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestBufferedMessages(t *testing.T) {
	t.Skip("broken atm")

	r, w := io.Pipe()

	sender := NewSender(w)
	receiver := NewReceiver(r)

	want := RandomContents(128)
	err := sender.Send(1, want)
	RequireNoError(t, err)

	got := <-receiver.Receive()

	switch v := got.(type) {
	case BufferedMessage:
		AssertSlicesEqual(t, v.Bytes(), want)
	default:
		t.Fatalf("unexpected type: %T", v)
	}
}
