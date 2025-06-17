package transport

import "io"

type (
	OperationID uint16

	Sender interface {
		Send(id OperationID, data []byte) error
		Stream(id OperationID, r io.Reader) error
	}

	Receiver interface {
		Receive() <-chan Message
	}

	Message interface {
		ID() OperationID
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

func (s *sender) Send(id OperationID, data []byte) error {
	return nil
}

func (s *sender) Stream(id OperationID, r io.Reader) error {
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
