package cluster

import (
	"bytes"
	"encoding/binary"
	"io"
)

type (
	Encoder interface {
		Encode(w io.Writer, data []byte) error
	}

	Decoder interface {
		Decode(r io.Reader) ([]byte, error)
	}
)

func NewEncoder() Encoder {
	return &encoder{
		varint: make([]byte, 4),
	}
}

type encoder struct {
	varint []byte
}

func (e *encoder) Encode(w io.Writer, data []byte) error {
	binary.LittleEndian.PutUint32(e.varint, uint32(len(data)))
	data = append(e.varint, data...)
	_, err := io.Copy(w, bytes.NewReader(data))
	return err
}

func NewDecoder() Decoder {
	return &decoder{
		varint: make([]byte, 4),
	}
}

type decoder struct {
	varint []byte
}

func (d decoder) Decode(r io.Reader) ([]byte, error) {
	_, err := io.ReadFull(r, d.varint)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, binary.LittleEndian.Uint32(d.varint))
	_, err = io.ReadFull(r, buf)
	return buf, err
}
