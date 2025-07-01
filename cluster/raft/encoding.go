package raft

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

	Encoder interface {
		Encode(r Record) error
	}

	Decoder interface {
		Decode() (Record, error)
	}
)

func NewEncoder(w io.Writer) Encoder {
	return &encoder{
		w:    w,
		hbuf: make([]byte, headerSize),
	}
}

type encoder struct {
	w    io.Writer
	hbuf []byte // header buffer
}

func (e *encoder) Encode(r Record) error {
	header{
		crc:   crc64.Checksum(r.Value, crc64Table),
		rtype: uint32(r.Type),
		size:  uint32(len(r.Value)),
	}.Put(e.hbuf)

	e.w.Write(e.hbuf)
	e.w.Write(r.Value)

	return nil
}

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
	d.r.Read(d.hbuf[:])
	header := parseHeader(d.hbuf[:])

	d.r.Read(d.vbuf[:header.size])

	if header.crc != crc64.Checksum(d.vbuf[:header.size], crc64Table) {
		return Record{}, ErrChecksumMismatch
	}

	return Record{
		Type:  RecordType(header.rtype),
		Value: d.vbuf[:header.size],
	}, nil
}

const (
	headerSize   = 16
	valueMaxSize = 128 << 10
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
