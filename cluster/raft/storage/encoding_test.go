package storage

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	got := Record{Value: make([]byte, 128)}
	want := randomRecord(128)
	buf := new(bytes.Buffer)

	written, err := NewEncoder(buf).Encode(want)
	AssertNoError(t, err)

	read, err := NewDecoder(buf).Decode(&got)
	AssertNoError(t, err).Require()
	AssertEqual(t, written, int64(headerSize+len(want.Value)))
	AssertEqual(t, written, read)
	AssertStructsEqual(t, got, want)
}

func TestDecodeFull(t *testing.T) {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	want := make([]Record, 10)
	for i := range want {
		want[i] = randomRecord(rand.Int64N(128) + 128)
		_, err := enc.Encode(want[i])
		AssertNoError(t, err).Require()
	}

	dec := NewDecoder(buf)
	record := Record{Value: make([]byte, 256)}
	for i := range want {
		n, err := dec.Decode(&record)
		AssertNoError(t, err).Require()
		AssertEqual(t, n, int64(headerSize+len(want[i].Value)))
	}

	// After reading every record, calling Decode again should return EOF.
	n, err := dec.Decode(&Record{})
	AssertErrorIs(t, err, io.EOF)
	AssertEqual(t, n, 0)
}

func TestEncodeDecodeErrorDetection(t *testing.T) {
	got := Record{Value: make([]byte, 128)}
	buf := new(bytes.Buffer)

	_, err := NewEncoder(buf).Encode(randomRecord(128))
	AssertNoError(t, err)

	// Tamper with the written data.
	buf.Truncate(100)

	_, err = NewDecoder(buf).Decode(&got)
	AssertErrorIs(t, err, ErrChecksumMismatch)
}

// Will revisit this when the API stabilizes.
// func TestEncodeDecodeDoesNotAllocate(t *testing.T) {
// 	buf := new(bytes.Buffer)
// 	encoder := NewEncoder(buf)
// 	decoder := NewDecoder(buf)

// 	src := randomRecord(128)
// 	dst := Record{Value: make([]byte, 128)}

// 	allocs := testing.AllocsPerRun(10, func() {
// 		encoder.Encode(src)
// 		decoder.Decode(&dst)
// 	})

// 	AssertEqual(t, allocs, 0)
// }

func randomRecord(n int64) Record {
	return Record{
		Type:  RecordType(rand.Uint32()),
		Value: RandomContents(n),
	}
}
