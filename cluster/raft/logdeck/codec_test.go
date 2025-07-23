package logdeck

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	got := new(Record)
	want := randomRecord(128)

	w := new(bytes.Buffer)
	written, err := NewEncoder(w).Encode(want)
	AssertNoError(t, err)

	r := bytes.NewReader(w.Bytes())
	read, err := NewDecoder(r).RecordAt(got, 0)
	AssertNoError(t, err).Require()

	AssertEqual(t, written, int64(RecordHeaderLength+len(want.Value)))
	AssertEqual(t, written, read)
	AssertDeepEqual(t, got, want)
}

func TestDecodeAllRecords(t *testing.T) {
	w := new(bytes.Buffer)
	enc := NewEncoder(w)
	want := make([]*Record, 10)

	for i := range want {
		want[i] = randomRecord(rand.Int64N(16) + 16)
		_, err := enc.Encode(want[i])
		AssertNoError(t, err).Require()
	}

	r := bytes.NewReader(w.Bytes())
	dec := NewDecoder(r)
	got := make([]*Record, len(want))

	pos := make([]int64, len(want))
	cursor := int64(0)
	for i := range want {
		got[i] = new(Record)
		n, err := dec.RecordAt(got[i], cursor)
		AssertNoError(t, err).Require()
		AssertEqual(t, n, int64(RecordHeaderLength+len(want[i].Value)))
		AssertDeepEqual(t, got[i], want[i])
		pos = append(pos, cursor)
		cursor += n
	}

	// Now try to read them out again in reverse.
	for i := len(pos) - 1; i <= 0; i-- {
		rec := new(Record)
		_, err := dec.RecordAt(got[i], pos[i])
		AssertNoError(t, err).Require()
		AssertDeepEqual(t, rec, got[i])
	}
}

func TestDecodeValue(t *testing.T) {
	want := randomRecord(rand.Int64N(128) + 128)

	w := new(bytes.Buffer)
	_, err := NewEncoder(w).Encode(want)
	AssertNoError(t, err)

	got := make([]byte, len(want.Value))
	r := bytes.NewReader(w.Bytes())
	_, err = NewDecoder(r).ValueAt(got, RecordHeaderLength)
	AssertNoError(t, err)
	AssertSlicesEqual(t, got, want.Value)
}

/*
Proving that yes, ValueAt is way, WAY faster. Especially for small values.

goos: linux
goarch: amd64
pkg: github.com/robinkb/cascade-registry/cluster/raft/storage
cpu: AMD Ryzen 7 7840U w/ Radeon  780M Graphics
BenchmarkDecode/RecordSize:_272,_RecordAt-16         	 2637667	       454.0 ns/op	 	599.17 MB/s
BenchmarkDecode/RecordSize:_272,_ValueAt-16            175557979	         6.702 ns/op	38199.31 MB/s
BenchmarkDecode/RecordSize:_1040,_RecordAt-16        	  675166	      1730 ns/op	 	601.21 MB/s
BenchmarkDecode/RecordSize:_1040,_ValueAt-16         	85390696	        11.90 ns/op		86035.17 MB/s
BenchmarkDecode/RecordSize:_32784,_RecordAt-16       	   81692	     14653 ns/op		2237.34 MB/s
BenchmarkDecode/RecordSize:_32784,_ValueAt-16        	 2650680	       449.6 ns/op		72876.08 MB/s
BenchmarkDecode/RecordSize:_65552,_RecordAt-16       	   42962	     27951 ns/op		2345.28 MB/s
BenchmarkDecode/RecordSize:_65552,_ValueAt-16        	 1331488	       900.6 ns/op		72770.52 MB/s
BenchmarkDecode/RecordSize:_131088,_RecordAt-16      	   21730	     55011 ns/op		2382.93 MB/s
BenchmarkDecode/RecordSize:_131088,_ValueAt-16       	  618614	      1795 ns/op		73029.26 MB/s
*/
func BenchmarkDecode(b *testing.B) {
	tc := []struct {
		record *Record
	}{
		{randomRecord(256)},
		{randomRecord(1 << 10)},
		{randomRecord(32 << 10)},
		{randomRecord(64 << 10)},
		{randomRecord(128 << 10)},
	}

	for _, tt := range tc {
		w := new(bytes.Buffer)
		NewEncoder(w).Encode(tt.record) // nolint: errcheck

		r := bytes.NewReader(w.Bytes())
		dec := NewDecoder(r)

		b.Run(fmt.Sprintf("RecordSize: %d, RecordAt", tt.record.Size()), func(b *testing.B) {
			record := new(Record)
			for b.Loop() {
				b.SetBytes(record.Size())
				dec.RecordAt(record, 0) // nolint: errcheck
			}
		})

		b.Run(fmt.Sprintf("RecordSize: %d, ValueAt", tt.record.Size()), func(b *testing.B) {
			p := make([]byte, len(tt.record.Value))
			size := int64(len(p))
			for b.Loop() {
				b.SetBytes(size)
				dec.ValueAt(p, RecordHeaderLength) // nolint: errcheck
			}
		})
	}
}

func TestEncodeDecodeErrorDetection(t *testing.T) {
	t.Run("truncated record leads to CRC mismatch", func(t *testing.T) {
		got := new(Record)
		w := new(bytes.Buffer)

		_, err := NewEncoder(w).Encode(randomRecord(128))
		AssertNoError(t, err)

		w.Truncate(100)

		r := bytes.NewReader(w.Bytes())
		_, err = NewDecoder(r).RecordAt(got, 0)
		AssertErrorIs(t, err, ErrShortRead)
	})

	t.Run("corrupt record leads to CRC mismatch", func(t *testing.T) {
		got := new(Record)
		w := new(bytes.Buffer)

		_, err := NewEncoder(w).Encode(randomRecord(128))
		AssertNoError(t, err).Require()

		b := w.Bytes()
		b[len(b)-10] = byte(255)
		w.Reset()
		w.Write(b)

		r := bytes.NewReader(w.Bytes())
		_, err = NewDecoder(r).RecordAt(got, 0)
		AssertErrorIs(t, err, ErrChecksumMismatch)
	})
}

func TestEncodeDecodeDoesNotAllocate(t *testing.T) {
	r, w := tempLog(t)
	encoder := NewEncoder(w)
	decoder := NewDecoder(r)

	src := randomRecord(128)
	dst := new(Record)

	allocs := testing.AllocsPerRun(10, func() {
		_, err := encoder.Encode(src)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.RecordAt(dst, 0)
		if err != nil {
			t.Fatal(err)
		}
	})

	AssertEqual(t, allocs, 0)
}

func randomRecord(n int64) *Record {
	return &Record{
		Type:  RecordType(rand.Uint32()),
		Value: RandomBytes(n),
	}
}
