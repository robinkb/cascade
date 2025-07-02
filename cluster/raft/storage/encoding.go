package storage

import (
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
	NVME = 0x9a6c9329ac4bc9b5
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
		Decode() (Record, error)
	}
)

// NewEncoder returns an Encoder that writes encoded Records to the io.Writer.
func NewEncoder(w io.Writer) Encoder {
	return &encoder{
		w: w,
	}
}

type encoder struct {
	w    io.Writer
	hbuf [headerSize]byte // header buffer
}

func (e *encoder) Encode(r Record) error {
	header{
		crc:   crc64.Checksum(r.Value, crc64Table),
		rtype: uint32(r.Type),
		size:  uint32(len(r.Value)),
	}.Put(e.hbuf[:])

	e.w.Write(e.hbuf[:])
	e.w.Write(r.Value)

	return nil
}

// NewDecoder returns a Decoder that reads decoded Records from the io.Reader.
func NewDecoder(r io.Reader) Decoder {
	return &decoder{
		r: r,
	}
}

type decoder struct {
	r    io.Reader
	hbuf [headerSize]byte   // header buffer
	vbuf [valueMaxSize]byte // value buffer
}

func (d *decoder) Decode() (Record, error) {
	_, err := d.r.Read(d.hbuf[:])
	if err != nil {
		return Record{}, err
	}

	header := parseHeader(d.hbuf[:])

	_, err = d.r.Read(d.vbuf[:header.size])

	if header.crc != crc64.Checksum(d.vbuf[:header.size], crc64Table) {
		return Record{}, ErrChecksumMismatch
	}

	return Record{
		Type:  RecordType(header.rtype),
		Value: d.vbuf[:header.size],
	}, err
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
