package raft

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
	"io"
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

	// record is the byte representation of an encoded Record.
	record struct {
		crc   [8]byte
		rtype [4]byte
		size  [4]byte
		value []byte
	}

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
		w: w,
	}
}

type encoder struct {
	w io.Writer
}

func (e *encoder) Encode(r Record) error {
	header := make([]byte, 16)

	binary.LittleEndian.PutUint64(header[0:8], crc64.Checksum(r.Value, crc64Table))
	binary.LittleEndian.PutUint32(header[8:12], uint32(r.Type))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(r.Value)))

	e.w.Write(header)
	e.w.Write(r.Value)

	return nil
}

func NewDecoder(r io.Reader) Decoder {
	return &decoder{
		r: r,
	}
}

type decoder struct {
	r io.Reader
}

func (d *decoder) Decode() (Record, error) {
	header := make([]byte, 16)
	d.r.Read(header)

	crc := binary.LittleEndian.Uint64(header[0:8])
	rtype := binary.LittleEndian.Uint32(header[8:12])
	size := binary.LittleEndian.Uint32(header[12:16])

	value := make([]byte, size)
	d.r.Read(value)

	if crc != crc64.Checksum(value, crc64Table) {
		return Record{}, ErrChecksumMismatch
	}

	return Record{
		Type:  RecordType(rtype),
		Value: value,
	}, nil
}
