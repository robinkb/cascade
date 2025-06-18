package transport

import (
	"bytes"
	"encoding/binary"
	"log"
)

type (
	BufferedEncoder interface {
		Encode(id MessageType, data []byte) ([]byte, error)
	}

	BufferedDecoder interface {
		Decode(data []byte) (MessageType, []byte, error)
	}
)

func NewBufferedEncoder() BufferedEncoder {
	return &bufferedEncoder{
		buf: bytes.NewBuffer(make([]byte, 0, 1024)),
	}
}

type bufferedEncoder struct {
	buf *bytes.Buffer
}

func (e *bufferedEncoder) Encode(id MessageType, data []byte) ([]byte, error) {
	header := header{
		magicNumber: headerMagicNumber,
		attributes:  0x00,
		messageType: uint16(id),
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

func (d *bufferedDecoder) Decode(data []byte) (MessageType, []byte, error) {
	header := parseHeader(data[:headerSize])
	return MessageType(header.messageType), data[headerSize:], nil
}

const (
	headerSize        = 64
	headerMagicNumber = 0xCA
)

func parseHeader(data []byte) header {
	if len(data) != headerSize {
		log.Panicln("invalid header length:", len(data))
	}

	return header{
		magicNumber: data[0],
		attributes:  data[1],
		messageType: binary.LittleEndian.Uint16(data[2:4]),
		length:      binary.LittleEndian.Uint32(data[4:8]),
	}
}

type header struct {
	magicNumber uint8
	attributes  uint8
	messageType uint16
	length      uint32
}

func (h header) bytes() []byte {
	buf := make([]byte, headerSize)
	buf[0] = h.magicNumber
	buf[1] = h.attributes
	binary.LittleEndian.PutUint16(buf[2:4], h.messageType)
	binary.LittleEndian.PutUint32(buf[4:8], h.length)
	return buf
}
