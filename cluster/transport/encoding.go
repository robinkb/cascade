package transport

import (
	"bytes"
	"encoding/binary"
	"log"
)

type (
	BufferedEncoder interface {
		Encode(id OperationID, data []byte) ([]byte, error)
	}

	BufferedDecoder interface {
		Decode(data []byte) (OperationID, []byte, error)
	}

	header struct {
		magicNumber uint8
		properties  uint8
		operationID uint16
		length      uint32
	}
)

func parseHeader(data []byte) header {
	if len(data) != 64 {
		log.Panicln("invalid header length:", len(data))
	}

	return header{
		magicNumber: data[0],
		properties:  data[1],
		operationID: binary.BigEndian.Uint16(data[2:4]),
		length:      binary.BigEndian.Uint32(data[4:8]),
	}
}

func (h header) bytes() []byte {
	buf := make([]byte, 64)
	buf[0] = h.magicNumber
	buf[1] = h.properties
	buf[2] = byte(h.operationID >> 8)
	buf[3] = byte(h.operationID)
	buf[4] = byte(h.length >> 24)
	buf[5] = byte(h.length >> 16)
	buf[6] = byte(h.length >> 8)
	buf[7] = byte(h.length)
	return buf
}

func NewBufferedEncoder() BufferedEncoder {
	return &bufferedEncoder{
		buf: bytes.NewBuffer(make([]byte, 0, 64)),
	}
}

type bufferedEncoder struct {
	buf *bytes.Buffer
}

func (e *bufferedEncoder) Encode(id OperationID, data []byte) ([]byte, error) {
	header := header{
		magicNumber: 0xCA,
		properties:  0x00,
		operationID: uint16(id),
		length:      uint32(len(data)),
	}

	e.buf.Reset()
	e.buf.Write(header.bytes())
	e.buf.Write(data)

	return e.buf.Bytes(), nil
}

func NewBufferedDecoder() BufferedDecoder {
	return &bufferedDecoder{}
}

type bufferedDecoder struct {
}

func (d *bufferedDecoder) Decode(data []byte) (OperationID, []byte, error) {
	header := parseHeader(data[:64])
	return OperationID(header.operationID), data[64:], nil
}
