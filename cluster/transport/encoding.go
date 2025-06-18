package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
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
		flags:       flags{},
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
	buf    [headerSize + payloadMaxSize]byte
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
			flags: flags{
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
	}

	mtype, err := dec.readHeader()
	if err != nil {
		return mtype, nil, err
	}

	dec.mtype = mtype

	return mtype, dec, nil
}

type streamDecoder struct {
	mtype  MessageType
	src    io.Reader
	hbuf   [headerSize]byte     // header buffer
	pbuf   [payloadMaxSize]byte // payload buffer
	cursor int
	size   int
}

func (d *streamDecoder) Read(p []byte) (int, error) {
	var err error
	if d.cursor == 0 {
		// Haven't read the header yet, or exhausted the buffer
		if d.size == 0 {
			_, err := d.readHeader()
			if err != nil {
				return 0, err
			}
		}

		n, err := d.src.Read(d.pbuf[:d.size])
		if err != nil && err != io.EOF {
			return 0, err
		}
		d.size = n
	}

	n := copy(p, d.pbuf[d.cursor:d.size])
	d.cursor += n
	if d.cursor == d.size {
		if d.size < cap(d.pbuf) {
			err = io.EOF
		} else {
			d.cursor = 0
			d.size = 0
		}
	}

	return n, err
}

func (d *streamDecoder) readHeader() (MessageType, error) {
	n, err := d.src.Read(d.hbuf[:])
	if err != nil {
		return 0, err
	}
	if n != headerSize {
		// Could probably retry here instead?
		return 0, errors.New("could not read header")
	}

	header := parseHeader(d.hbuf[:])
	d.size = int(header.length)
	return MessageType(header.messageType), err
}

const (
	headerSize        = 8 // Size of the header in bytes
	headerMagicNumber = 0xCA
	payloadMaxSize    = 32<<10 - headerSize
)

func parseHeader(data []byte) header {
	if len(data) != headerSize {
		log.Panicln("invalid header length:", len(data))
	}

	return header{
		magicNumber: data[0],
		flags:       parseFlags(data[1]),
		messageType: binary.LittleEndian.Uint16(data[2:4]),
		length:      binary.LittleEndian.Uint32(data[4:8]),
	}
}

type header struct {
	magicNumber uint8
	flags       flags
	messageType uint16
	length      uint32
}

func (h header) Put(b []byte) {
	if len(b) != headerSize {
		panic("expected byte slice of length 8")
	}
	b[0] = h.magicNumber
	b[1] = h.flags.Byte()
	binary.LittleEndian.PutUint16(b[2:4], h.messageType)
	binary.LittleEndian.PutUint32(b[4:8], h.length)
}

func (h header) Bytes() []byte {
	buf := make([]byte, headerSize)
	h.Put(buf)
	return buf
}

func parseFlags(b byte) flags {
	var f flags
	if b>>7 == 1 {
		f.stream = true
	}
	return f
}

type flags struct {
	stream bool
}

func (f flags) Byte() byte {
	var b byte
	if f.stream {
		b += 0b10000000
	}
	return b
}
