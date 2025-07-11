package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"
	"io"
	"log"
)

const (
	// The NVME polynomial (reversed, as used by Go).
	// Apparently differences exist in performance (and other attributes)
	// between polynomials. I should do some benchmarks.
	NVME = 0x9A6C9329AC4BC9B5
	// Length of the header of a Record in bytes.
	RecordHeaderLength = 16
)

var (
	crc64Table = crc64.MakeTable(NVME)
)

type (
	// Encoder writes encoded Records to the output stream.
	// It is not threadsafe.
	Encoder interface {
		// Encode writes a Record to the output stream.
		// It returns the amount of bytes written and an error, if any.
		// The written amount can be used to track positions of records.
		Encode(r *Record) (int64, error)
	}

	// Decoder reads decoded Records from the input stream.
	Decoder interface {
		// DecodeAt is a wrapper around an io.ReaderAt for decoding Records.
		// It returns the amount of bytes read and an error, if any.
		DecodeAt(r *Record, off int64) (int64, error)
	}
)

type RecordType uint32

type Record struct {
	Type  RecordType
	Value []byte
}

func (r *Record) Size() int64 {
	return int64(RecordHeaderLength + len(r.Value))
}

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

func (e *encoder) Encode(r *Record) (int64, error) {
	e.buf.Reset()
	e.buf.Grow(RecordHeaderLength + len(r.Value))

	hbuf := e.buf.Bytes()[:RecordHeaderLength]

	// Writing starting at byte 8 to leave room for the CRC.
	binary.LittleEndian.PutUint32(hbuf[8:12], uint32(r.Type))
	binary.LittleEndian.PutUint32(hbuf[12:16], uint32(len(r.Value)))

	e.buf.Write(hbuf)
	e.buf.Write(r.Value)

	crc := crc64.Checksum(e.buf.Bytes()[crc64.Size:], crc64Table)
	binary.LittleEndian.PutUint64(e.buf.Bytes(), crc)

	return io.Copy(e.dst, e.buf)
}

// NewDecoder returns a Decoder that reads decoded Records from the io.ReaderAt.
func NewDecoder(r io.ReaderAt) Decoder {
	return &decoder{
		src: r,
		buf: new(bytes.Buffer),
	}
}

type decoder struct {
	src io.ReaderAt
	buf *bytes.Buffer
}

func (d *decoder) DecodeAt(r *Record, off int64) (int64, error) {
	d.buf.Reset()
	var read int64

	// Reading and parsing the header.
	d.buf.Grow(RecordHeaderLength)
	hbuf := d.buf.Bytes()[:RecordHeaderLength]
	n, err := d.src.ReadAt(hbuf, off)
	read += int64(n)
	if err != nil {
		return read, err
	}
	// Ensure header is available in the buffer for later CRC calculation,
	// which includes parts of the header.
	d.buf.Write(hbuf)
	header := parseHeader(hbuf)

	// The decoder normally keeps reading until EOF.
	// But with a pre-allocated file, a lot of the file might be empty.
	// Treat reading an empty header the same as EOF in that case.
	if header.isEmpty() {
		return 0, io.EOF
	}

	// Reading the payload.
	d.buf.Grow(RecordHeaderLength + int(header.size))
	pbuf := d.buf.Bytes()[RecordHeaderLength : RecordHeaderLength+header.size]
	n, err = d.src.ReadAt(pbuf, off+RecordHeaderLength)
	read += int64(n)
	if err != nil {
		// An EOF is not expected here. It would indicate a short read,
		// and thus definitely a checksum mismatch.
		// TODO: This could actually indicate unrecoverable corruption of the log.
		// Another error might be warranted here. Or panic.
		if err == io.EOF {
			return read, ErrShortRead
		}
		return read, err
	}
	d.buf.Write(pbuf)

	// CRC check on the entire record except for the CRC itself.
	if header.crc != crc64.Checksum(d.buf.Bytes()[crc64.Size:], crc64Table) {
		return read, ErrChecksumMismatch
	}

	// Placing values into the Record.
	r.Type = RecordType(header.rtype)
	r.Value = d.buf.Bytes()[RecordHeaderLength : RecordHeaderLength+header.size]

	return read, nil
}

func parseHeader(b []byte) header {
	if len(b) != RecordHeaderLength {
		log.Panicf("invalid header length; got %d while expecting %d", len(b), RecordHeaderLength)
	}

	return header{
		crc:   binary.LittleEndian.Uint64(b[0:8]),
		rtype: binary.LittleEndian.Uint32(b[8:12]),
		size:  binary.LittleEndian.Uint32(b[12:16]),
	}
}

type header struct {
	crc   uint64 // 8 bytes
	rtype uint32 // 4 bytes
	size  uint32 // 4 bytes
}

func (h *header) isEmpty() bool {
	return h.crc == 0 && h.rtype == 0 && h.size == 0
}
