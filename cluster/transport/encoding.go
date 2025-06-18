package transport

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
)

type (
	Encoder interface {
		Encode(id MessageType, data []byte) ([]byte, error)
		EncodeStream(id MessageType, r io.Reader) (io.Reader, error)
	}

	Decoder interface {
		Decode(data []byte) (MessageType, []byte, error)
		DecodeStream(r io.Reader) (MessageType, io.Reader, error)
	}
)

func NewBufferedEncoder() Encoder {
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
		attributes:  attributes{},
		messageType: uint16(id),
		length:      uint32(len(data)),
	}

	e.buf.Reset()
	e.buf.Write(header.Bytes())
	e.buf.Write(data)

	return e.buf.Bytes(), nil
}

func (e *bufferedEncoder) EncodeStream(id MessageType, r io.Reader) (io.Reader, error) {
	return &streamEncoder{
		mtype: id,
		src:   r,
		buf:   bytes.NewBuffer(make([]byte, 1<<10)),
		msg:   bytes.NewBuffer(make([]byte, 0)),
	}, nil
}

type streamEncoder struct {
	mtype MessageType
	src   io.Reader
	buf   *bytes.Buffer
	msg   *bytes.Buffer
	done  bool
}

func (e *streamEncoder) Read(p []byte) (int, error) {
	// Buffer is empty, have to read from the source.
	if e.msg.Len() == 0 && !e.done {
		e.buf.Reset()
		n, err := io.CopyN(e.buf, e.src, int64(e.buf.Cap()))
		if err != nil {
			if err != io.EOF {
				return int(n), err
			}
			e.done = true
		}

		header := header{
			magicNumber: headerMagicNumber,
			attributes: attributes{
				stream: true,
			},
			messageType: uint16(e.mtype),
			length:      uint32(n),
		}

		e.msg.Write(header.Bytes())
		e.msg.Write(e.buf.Bytes())
	}

	return e.msg.Read(p)
}

func NewBufferedDecoder() Decoder {
	return &bufferedDecoder{}
}

type bufferedDecoder struct {
}

func (d *bufferedDecoder) Decode(data []byte) (MessageType, []byte, error) {
	header := parseHeader(data[:headerSize])
	return MessageType(header.messageType), data[headerSize:], nil
}

func (d *bufferedDecoder) DecodeStream(r io.Reader) (MessageType, io.Reader, error) {
	buf := make([]byte, headerSize)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		log.Println(err)
	}
	header := parseHeader(buf)
	data := make([]byte, 0, header.length)
	io.ReadFull(r, data)

	return MessageType(header.messageType), &streamDecoder{
		mtype: MessageType(header.messageType),
		src:   r,
		buf:   bytes.NewBuffer(data),
	}, nil
}

type streamDecoder struct {
	mtype MessageType
	src   io.Reader
	buf   *bytes.Buffer
}

func (d *streamDecoder) Read(p []byte) (int, error) {
	return d.src.Read(p)
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
		attributes:  parseAttributes(data[1]),
		messageType: binary.LittleEndian.Uint16(data[2:4]),
		length:      binary.LittleEndian.Uint32(data[4:8]),
	}
}

type header struct {
	magicNumber uint8
	attributes  attributes
	messageType uint16
	length      uint32
}

func (h header) Bytes() []byte {
	buf := make([]byte, headerSize)
	buf[0] = h.magicNumber
	buf[1] = h.attributes.Byte()
	binary.LittleEndian.PutUint16(buf[2:4], h.messageType)
	binary.LittleEndian.PutUint32(buf[4:8], h.length)
	return buf
}

func parseAttributes(b byte) attributes {
	var a attributes
	if b>>7 == 1 {
		a.stream = true
	}
	return a
}

type attributes struct {
	stream bool
}

func (a attributes) Byte() byte {
	var b byte
	if a.stream {
		b += 0b10000000
	}
	return b
}
