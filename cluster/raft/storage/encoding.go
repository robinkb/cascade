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
		// It returns the amount of bytes written and an error, if any.
		// The written amount can be used to track positions of records.
		Encode(r Record) (int64, error)
	}

	// Decoder reads decoded Records from the input stream.
	// It is not threadsafe.
	Decoder interface {
		// Decode decodes a Record from the stream into r.
		// It returns the amount of bytes read and an error, if any.
		// The read amount can be used to track positions of records.
		Decode(r *Record) (int64, error)
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

func (e *encoder) Encode(r Record) (int64, error) {
	var written int64
	buf := make([]byte, headerSize+len(r.Value))

	header{
		rtype: uint32(r.Type),
		size:  uint32(len(r.Value)),
	}.Put(buf[:headerSize])

	n := copy(buf[headerSize:], r.Value)
	if n != len(r.Value) {
		return 0, io.ErrShortWrite
	}

	crc := crc64.Checksum(buf[crc64.Size:], crc64Table)
	binary.LittleEndian.PutUint64(buf, crc)

	n, err := e.dst.Write(buf)
	written += int64(n)

	return written, err
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

func (d *decoder) Decode(r *Record) (int64, error) {
	d.buf.Reset()

	var read int64
	n, err := io.CopyN(d.buf, d.src, headerSize)
	read += n
	if err != nil {
		return read, err
	}

	header := parseHeader(d.buf.Bytes())

	d.buf.Grow(int(header.size))
	n, err = io.CopyN(d.buf, d.src, int64(header.size))
	read += n

	if header.crc != crc64.Checksum(d.buf.Bytes()[crc64.Size:], crc64Table) {
		return read, ErrChecksumMismatch
	}

	r.Type = RecordType(header.rtype)
	n = int64(copy(r.Value, d.buf.Bytes()[headerSize:]))
	if n != int64(header.size) {
		return read, io.ErrShortWrite
	}

	return read, err
}

const (
	headerSize = 16
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
