package storage

import (
	"bytes"
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	got := Record{Value: make([]byte, 128)}
	want := randomRecord(128)
	buf := new(bytes.Buffer)

	err := NewEncoder(buf).Encode(want)
	AssertNoError(t, err)

	err = NewDecoder(buf).Decode(&got)
	AssertNoError(t, err).Require()
	AssertStructsEqual(t, got, want)
}

func TestEncodeDecodeErrorDetection(t *testing.T) {
	got := Record{Value: make([]byte, 128)}
	buf := new(bytes.Buffer)

	err := NewEncoder(buf).Encode(randomRecord(128))
	AssertNoError(t, err)

	// Tamper with the written data.
	buf.Truncate(100)

	err = NewDecoder(buf).Decode(&got)
	AssertErrorIs(t, err, ErrChecksumMismatch)
}

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
