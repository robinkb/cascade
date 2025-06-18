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
	}, nil
}

type streamEncoder struct {
	mtype  MessageType
	src    io.Reader
	buf    [32 << 10]byte
	cursor int
	size   int
}

func (e *streamEncoder) Read(p []byte) (int, error) {
	var err error
	if e.cursor == 0 {
		n, err := e.src.Read(e.buf[headerSize:cap(e.buf)])
		if err != nil {
			return 0, err
		}

		header := header{
			magicNumber: headerMagicNumber,
			attributes: attributes{
				stream: true,
			},
			messageType: uint16(e.mtype),
			length:      uint32(n),
		}
		header.Put(e.buf[:headerSize])
		e.size = headerSize + n
	}

	n := copy(p, e.buf[e.cursor:e.size])
	e.cursor += n
	if e.cursor == e.size {
		e.cursor = 0
		if e.size < cap(e.buf)-headerSize {
			err = io.EOF
		}
	}

	return n, err
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
	dec := &streamDecoder{
		src: r,
		buf: bytes.NewBuffer(make([]byte, 0, 1024)),
	}

	_, err := io.CopyN(dec.buf, dec.src, headerSize)
	if err != nil {
		if err != io.EOF {
			return 0, nil, err
		}
		dec.done = true
	}

	header := parseHeader(dec.buf.Bytes())
	dec.buf.Reset()
	_, err = io.CopyN(dec.buf, dec.src, int64(header.length))
	if err != nil {
		if err != io.EOF {
			return 0, nil, err
		}
		dec.done = true
	}
	return MessageType(header.messageType), dec, nil
}

type streamDecoder struct {
	mtype MessageType
	src   io.Reader
	buf   *bytes.Buffer
	done  bool
}

func (d *streamDecoder) Read(p []byte) (int, error) {
	// Buffer is empty, have to read from the source.
	if d.buf.Len() == 0 && !d.done {
		d.buf.Reset()
		n, err := io.CopyN(d.buf, d.src, headerSize)
		if err != nil {
			if err != io.EOF {
				return int(n), err
			}
			d.done = true
		}

		if !d.done {
			header := parseHeader(d.buf.Bytes())
			d.buf.Reset()
			n, err = io.CopyN(d.buf, d.src, int64(header.length))
			if err != nil {
				if err != io.EOF {
					return int(n), err
				}
				d.done = true
			}
		}
	}

	return d.buf.Read(p)
}

const (
	headerSize        = 8 // Size of the header in bytes
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

func (h header) Put(b []byte) {
	if len(b) != headerSize {
		panic("expected byte slice of length 8")
	}
	b[0] = h.magicNumber
	b[1] = h.attributes.Byte()
	binary.LittleEndian.PutUint16(b[2:4], h.messageType)
	binary.LittleEndian.PutUint32(b[4:8], h.length)
}

func (h header) Bytes() []byte {
	buf := make([]byte, headerSize)
	h.Put(buf)
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
