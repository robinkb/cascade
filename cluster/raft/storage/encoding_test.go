package storage_test

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	got := storage.Record{Value: make([]byte, 128)}
	want := randomRecord(128)

	w := new(bytes.Buffer)
	written, err := storage.NewEncoder(w).Encode(want)
	AssertNoError(t, err)

	r := bytes.NewReader(w.Bytes())
	read, err := storage.NewDecoder(r).Decode(&got)
	AssertNoError(t, err).Require()

	AssertEqual(t, written, int64(storage.RecordHeaderLength+len(want.Value)))
	AssertEqual(t, written, read)
	AssertStructsEqual(t, got, want)
}

func TestDecodeAllRecords(t *testing.T) {
	w := new(bytes.Buffer)
	enc := storage.NewEncoder(w)

	want := make([]storage.Record, 10)
	for i := range want {
		want[i] = randomRecord(rand.Int64N(16) + 16)
		_, err := enc.Encode(want[i])
		AssertNoError(t, err).Require()
	}

	r := bytes.NewReader(w.Bytes())
	dec := storage.NewDecoder(r)

	record := storage.Record{Value: make([]byte, 64)}
	for i := range want {
		n, err := dec.Decode(&record)
		AssertNoError(t, err).Require()
		AssertEqual(t, n, int64(storage.RecordHeaderLength+len(want[i].Value)))
		AssertStructsEqual(t, record, want[i])
	}

	// After reading every record, calling Decode again should return EOF.
	n, err := dec.Decode(&storage.Record{})
	AssertErrorIs(t, err, io.EOF)
	AssertEqual(t, n, 0)
}

func TestDecodeSeek(t *testing.T) {
	w := new(bytes.Buffer)
	enc := storage.NewEncoder(w)

	want := make([]storage.Record, 10)
	pos := make([]int64, len(want))
	var cursor int64
	for i := range want {
		want[i] = randomRecord(rand.Int64N(16) + 16)
		n, err := enc.Encode(want[i])
		AssertNoError(t, err).Require()

		pos[i] = cursor
		cursor += n
	}

	r := bytes.NewReader(w.Bytes())
	dec := storage.NewDecoder(r)

	record := storage.Record{Value: make([]byte, 128)}
	for i := len(want) - 1; i >= 0; i-- {
		dec.Seek(pos[i], io.SeekStart)
		_, err := dec.Decode(&record)
		AssertNoError(t, err)
		AssertStructsEqual(t, record, want[i])
	}
}

func TestEncodeDecodeErrorDetection(t *testing.T) {
	got := storage.Record{Value: make([]byte, 128)}
	w := new(bytes.Buffer)

	_, err := storage.NewEncoder(w).Encode(randomRecord(128))
	AssertNoError(t, err)

	// Tamper with the written data.
	w.Truncate(100)

	r := bytes.NewReader(w.Bytes())
	_, err = storage.NewDecoder(r).Decode(&got)
	AssertErrorIs(t, err, storage.ErrChecksumMismatch)
}

// func TestEncodeDecodeDoesNotAllocate(t *testing.T) {
// 	// Still allocates twice in Decode as of writing.
// 	// Will revisit this when the API stabilizes.
// 	t.SkipNow()

// 	buf := make([])
// 	bytes.NewReader()
// 	bufio.NewReadWriter()
// 	w := new(bytes.Buffer)
// 	encoder := storage.NewEncoder(w)
// 	decoder := NewDecoder(buf)

// 	src := randomRecord(128)
// 	dst := Record{Value: make([]byte, 256)}

// 	allocs := testing.AllocsPerRun(10, func() {
// 		encoder.Encode(src)
// 		decoder.Decode(&dst)
// 	})

// 	AssertEqual(t, allocs, 0)
// }

func randomRecord(n int64) storage.Record {
	return storage.Record{
		Type:  storage.RecordType(rand.Uint32()),
		Value: RandomContents(n),
	}
}
