package transport

import "io"

type (
	MessageType uint16

	Sender interface {
		Send(t MessageType, data []byte) error
		Stream(t MessageType, r io.Reader) error
	}

	Receiver interface {
		Receive() <-chan Message
	}

	Message interface {
		Type() MessageType
	}

	StreamingMessage interface {
		Message
		io.Reader
	}

	BufferedMessage interface {
		Message
		Bytes() []byte
	}
)

func NewSender(w io.Writer) Sender {
	return &sender{
		w: w,
	}
}

type sender struct {
	w io.Writer
}

func (s *sender) Send(id MessageType, data []byte) error {
	return nil
}

func (s *sender) Stream(id MessageType, r io.Reader) error {
	return nil
}

func NewReceiver(r io.Reader) Receiver {
	return &receiver{
		r: r,
	}
}

type receiver struct {
	r io.Reader
}

func (r *receiver) Receive() <-chan Message {
	return nil
}
