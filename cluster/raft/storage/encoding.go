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
		buf: new(bytes.Buffer),
	}
}

type encoder struct {
	dst io.Writer
	buf *bytes.Buffer
}

func (e *encoder) Encode(r Record) (int64, error) {
	e.buf.Reset()
	e.buf.Grow(headerSize + len(r.Value))

	header := e.buf.AvailableBuffer()[:headerSize]

	// Writing starting at byte 8 to leave room for the CRC.
	binary.LittleEndian.PutUint32(header[8:12], uint32(r.Type))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(r.Value)))

	e.buf.Write(header)
	e.buf.Write(r.Value)

	crc := crc64.Checksum(e.buf.Bytes()[crc64.Size:], crc64Table)
	binary.LittleEndian.PutUint64(e.buf.Bytes(), crc)

	return io.Copy(e.dst, e.buf)
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
