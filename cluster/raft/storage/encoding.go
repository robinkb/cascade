package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"io"
	"log"
)

const (
	// The NVME polynomial (reversed, as used by Go).
	// Apparently differences exist in performance (and other attributes)
	// between polynomials. I should do some benchmarks.
	NVME = 0x9A6C9329AC4BC9B5
)

var (
	crc64Table = crc64.MakeTable(NVME)

	ErrChecksumMismatch = errors.New("CRC checksums did not match")
)

type (
	RecordType uint32

	Record struct {
		Type  RecordType
		Value []byte
	}

	// Encoder writes encoded Records to the output stream.
	// It is not threadsafe.
	Encoder interface {
		// Encode writes a Record to the output stream.
		Encode(r Record) error
	}

	// Decoder reads decoded Records from the input stream.
	// It is not threadsafe.
	Decoder interface {
		// Decode returns a decoded Record from the input stream.
		// The returned Record's Value is only valid until the next
		// call to Decode(). The Value should be unmarshalled or copied
		// immediately.
		Decode(r *Record) error
	}
)

// NewEncoder returns an Encoder that writes encoded Records to the io.Writer.
func NewEncoder(w io.Writer) Encoder {
	return &encoder{
		dst: w,
	}
}

type encoder struct {
	dst io.Writer
}

func (e *encoder) Encode(r Record) error {
	buf := make([]byte, headerSize+len(r.Value))

	header{
		rtype: uint32(r.Type),
		size:  uint32(len(r.Value)),
	}.Put(buf[:headerSize])

	copy(buf[headerSize:], r.Value)

	crc := crc64.Checksum(buf[8:], crc64Table)
	binary.LittleEndian.PutUint64(buf, crc)

	e.dst.Write(buf)

	return nil
}

// NewDecoder returns a Decoder that reads decoded Records from the io.Reader.
func NewDecoder(r io.Reader) Decoder {
	return &decoder{
		src: r,
		buf: new(bytes.Buffer),
	}
}

type decoder struct {
	src io.Reader
	buf *bytes.Buffer
}

func (d *decoder) Decode(r *Record) error {
	d.buf.Reset()

	io.CopyN(d.buf, d.src, headerSize)
	header := parseHeader(d.buf.Bytes())

	d.buf.Grow(int(header.size))
	io.CopyN(d.buf, d.src, int64(header.size))

	// TODO: CRC should cover everything but itself.
	if header.crc != crc64.Checksum(d.buf.Bytes()[8:], crc64Table) {
		return ErrChecksumMismatch
	}

	r.Type = RecordType(header.rtype)
	copy(r.Value, d.buf.Bytes()[headerSize:])

	return nil
}

const (
	headerSize   = 16
	valueMaxSize = 128 << 20
)

func parseHeader(b []byte) header {
	if len(b) != headerSize {
		log.Panicf("invalid header length; got %d while expecting %d", len(b), headerSize)
	}

	return header{
		crc:   binary.LittleEndian.Uint64(b[0:8]),
		rtype: binary.LittleEndian.Uint32(b[8:12]),
		size:  binary.LittleEndian.Uint32(b[12:16]),
	}
}

type header struct {
	crc   uint64
	rtype uint32
	size  uint32
}

func (h header) Put(b []byte) {
	if len(b) != headerSize {
		log.Panicf("expected byte slice of length %d but got %d", headerSize, len(b))
	}

	binary.LittleEndian.PutUint64(b[0:8], h.crc)
	binary.LittleEndian.PutUint32(b[8:12], h.rtype)
	binary.LittleEndian.PutUint32(b[12:16], h.size)
}
