package qwal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"math"
)

const (
	// RecordHeaderLength is the length of a Record header in bytes.
	RecordHeaderLength = 16
	// MaximumValueSize is the maximum length of a value, in bytes.
	MaximumValueSize = math.MaxUint32
)

var (
	crc64Table = crc64.MakeTable(crc64.ECMA)
)

type record struct {
	Type  Type
	Value []byte
}

func (r *record) size() int64 {
	return int64(RecordHeaderLength + len(r.Value))
}

// newEncoder returns an Encoder that writes encoded Records to the io.Writer.
func newEncoder(w io.WriteSeeker) *encoder {
	return &encoder{
		dst: w,
		buf: new(bytes.Buffer),
	}
}

// encoder writes encoded Records to the output stream.
// It is not threadsafe.
type encoder struct {
	dst io.WriteSeeker
	buf *bytes.Buffer
}

// Encode writes a Record to the output stream.
// It returns the amount of bytes written and an error, if any.
// The written amount can be used to track positions of records.
func (e *encoder) Encode(r *record) (int64, error) {
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

	n, err := e.dst.Write(e.buf.Bytes())
	return int64(n), err
}

// Seek wraps the io.Seek method of the Encoder's backing io.WriteSeeker.
func (e *encoder) Seek(offset int64, whence int) (int64, error) {
	return e.dst.Seek(offset, whence)
}

// newDecoder returns a Decoder that reads decoded Records from the io.ReaderAt.
func newDecoder(r io.ReaderAt) *decoder {
	return &decoder{
		src: r,
		buf: new(bytes.Buffer),
	}
}

// decoder reads decoded Records from the input stream.
type decoder struct {
	src io.ReaderAt
	buf *bytes.Buffer
}

// RecordAt is a wrapper around an io.ReaderAt for decoding Records.
// It returns the amount of bytes read and an error, if any.
func (d *decoder) RecordAt(r *record, off int64) (int64, error) {
	d.buf.Reset()
	var read int64

	// Reading and parsing the header.
	d.buf.Grow(RecordHeaderLength)
	hbuf := d.buf.Bytes()[:RecordHeaderLength]
	n, err := d.src.ReadAt(hbuf, off)
	read += int64(n)
	if err != nil {
		return read, fmt.Errorf("could not read header: %w", err)
	}
	// Ensure header is available in the buffer for later CRC calculation,
	// which includes parts of the header.
	d.buf.Write(hbuf)
	header := parseHeader(hbuf)

	// The decoder normally keeps reading until EOF.
	// But with a pre-allocated file, a lot of the file might be empty.
	// Treat reading an empty header the same as EOF in that case.
	if header.isEmpty() {
		return read, fmt.Errorf("%w (empty header)", io.EOF)
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
		return read, fmt.Errorf("failed to read payload: %w", err)
	}
	d.buf.Write(pbuf)

	// CRC check on the entire record except for the CRC itself.
	if header.crc != crc64.Checksum(d.buf.Bytes()[crc64.Size:], crc64Table) {
		return read, ErrChecksumMismatch
	}

	// Placing values into the Record.
	r.Type = Type(header.rtype)
	r.Value = d.buf.Bytes()[RecordHeaderLength : RecordHeaderLength+header.size]

	return read, nil
}

// ValueAt is a wrapper around an io.ReaderAt for reading Record values.
// Its behavior is the same as io.ReaderAt.
func (d *decoder) ValueAt(p []byte, off int64) (int, error) {
	return d.src.ReadAt(p, off)
}

func parseHeader(b []byte) header {
	if len(b) != RecordHeaderLength {
		panic(fmt.Sprintf("invalid header length; got %d while expecting %d", len(b), RecordHeaderLength))
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
