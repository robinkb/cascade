package storage_test

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	got := new(storage.Record)
	want := randomRecord(128)

	w := new(bytes.Buffer)
	written, err := storage.NewEncoder(w).Encode(want)
	AssertNoError(t, err)

	r := bytes.NewReader(w.Bytes())
	read, err := storage.NewDecoder(r).DecodeAt(got, 0)
	AssertNoError(t, err).Require()

	AssertEqual(t, written, int64(storage.RecordHeaderLength+len(want.Value)))
	AssertEqual(t, written, read)
	AssertStructsEqual(t, got, want)
}

func TestDecodeAllRecords(t *testing.T) {
	w := new(bytes.Buffer)
	enc := storage.NewEncoder(w)
	want := make([]*storage.Record, 10)

	for i := range want {
		want[i] = randomRecord(rand.Int64N(16) + 16)
		_, err := enc.Encode(want[i])
		AssertNoError(t, err).Require()
	}

	r := bytes.NewReader(w.Bytes())
	dec := storage.NewDecoder(r)
	got := make([]*storage.Record, len(want))

	pos := make([]int64, len(want))
	cursor := int64(0)
	for i := range want {
		got[i] = new(storage.Record)
		n, err := dec.DecodeAt(got[i], cursor)
		AssertNoError(t, err).Require()
		AssertEqual(t, n, int64(storage.RecordHeaderLength+len(want[i].Value)))
		AssertStructsEqual(t, got[i], want[i])
		pos = append(pos, cursor)
		cursor += n
	}

	// Now try to read them out again in reverse.
	for i := len(pos) - 1; i <= 0; i-- {
		rec := new(storage.Record)
		_, err := dec.DecodeAt(got[i], pos[i])
		AssertNoError(t, err).Require()
		AssertStructsEqual(t, rec, got[i])
	}
}

func TestEncodeDecodeErrorDetection(t *testing.T) {
	t.Run("truncated record leads to CRC mismatch", func(t *testing.T) {
		got := new(storage.Record)
		w := new(bytes.Buffer)

		_, err := storage.NewEncoder(w).Encode(randomRecord(128))
		AssertNoError(t, err)

		w.Truncate(100)

		r := bytes.NewReader(w.Bytes())
		_, err = storage.NewDecoder(r).DecodeAt(got, 0)
		AssertErrorIs(t, err, storage.ErrChecksumMismatch)
	})

	t.Run("corrupt record leads to CRC mismatch", func(t *testing.T) {
		got := new(storage.Record)
		w := new(bytes.Buffer)

		_, err := storage.NewEncoder(w).Encode(randomRecord(128))
		AssertNoError(t, err).Require()

		b := w.Bytes()
		b[len(b)-10] = byte(255)
		w.Reset()
		w.Write(b)

		r := bytes.NewReader(w.Bytes())
		_, err = storage.NewDecoder(r).DecodeAt(got, 0)
		AssertErrorIs(t, err, storage.ErrChecksumMismatch)
	})
}

func TestEncodeDecodeDoesNotAllocate(t *testing.T) {
	r, w := tempLog(t)
	encoder := storage.NewEncoder(w)
	decoder := storage.NewDecoder(r)

	src := randomRecord(128)
	dst := new(storage.Record)

	allocs := testing.AllocsPerRun(10, func() {
		_, err := encoder.Encode(src)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.DecodeAt(dst, 0)
		if err != nil {
			t.Fatal(err)
		}
	})

	AssertEqual(t, allocs, 0)
}

func randomRecord(n int64) *storage.Record {
	return &storage.Record{
		Type:  storage.RecordType(rand.Uint32()),
		Value: RandomContents(n),
	}
}
