package storage

import (
	"bytes"
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	want := randomRecord(128)
	buf := new(bytes.Buffer)

	err := NewEncoder(buf).Encode(want)
	AssertNoError(t, err)

	got, err := NewDecoder(buf).Decode()
	AssertNoError(t, err)
	AssertStructsEqual(t, got, want)
}

func TestEncodeDecodeErrorDetection(t *testing.T) {
	buf := new(bytes.Buffer)

	err := NewEncoder(buf).Encode(randomRecord(128))
	AssertNoError(t, err)

	// Tamper with the written data.
	buf.Truncate(100)

	_, err = NewDecoder(buf).Decode()
	AssertErrorIs(t, err, ErrChecksumMismatch)
}

func TestEncodeDecodeDoesNotAllocate(t *testing.T) {
	buf := new(bytes.Buffer)
	encoder := NewEncoder(buf)
	decoder := NewDecoder(buf)

	record := randomRecord(128)

	allocs := testing.AllocsPerRun(10, func() {
		encoder.Encode(record)
		decoder.Decode()
	})

	AssertEqual(t, allocs, 0)
}

func randomRecord(n int64) Record {
	return Record{
		Type:  RecordType(rand.Uint32()),
		Value: RandomContents(n),
	}
}
