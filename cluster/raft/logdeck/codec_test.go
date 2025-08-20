package logdeck

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	r, w := testReaderWriter(t)
	got := new(record)
	want := randomRecord(128)

	written, err := newEncoder(w).Encode(want)
	AssertNoError(t, err)

	read, err := newDecoder(r).RecordAt(got, 0)
	AssertNoError(t, err).Require()

	AssertEqual(t, written, int64(RecordHeaderLength+len(want.Value)))
	AssertEqual(t, written, read)
	AssertDeepEqual(t, got, want)
}

func TestDecodeAllRecords(t *testing.T) {
	r, w := testReaderWriter(t)

	enc := newEncoder(w)
	want := make([]*record, 10)

	for i := range want {
		want[i] = randomRecord(rand.Int64N(16) + 16)
		_, err := enc.Encode(want[i])
		AssertNoError(t, err).Require()
	}

	dec := newDecoder(r)
	got := make([]*record, len(want))

	pos := make([]int64, len(want))
	cursor := int64(0)
	for i := range want {
		got[i] = new(record)
		n, err := dec.RecordAt(got[i], cursor)
		AssertNoError(t, err).Require()
		AssertEqual(t, n, int64(RecordHeaderLength+len(want[i].Value)))
		AssertDeepEqual(t, got[i], want[i])
		pos = append(pos, cursor)
		cursor += n
	}

	// Now try to read them out again in reverse.
	for i := len(pos) - 1; i <= 0; i-- {
		rec := new(record)
		_, err := dec.RecordAt(got[i], pos[i])
		AssertNoError(t, err).Require()
		AssertDeepEqual(t, rec, got[i])
	}
}

func TestDecodeValue(t *testing.T) {
	r, w := testReaderWriter(t)
	want := randomRecord(rand.Int64N(128) + 128)
	got := make([]byte, len(want.Value))

	_, err := newEncoder(w).Encode(want)
	AssertNoError(t, err)

	_, err = newDecoder(r).ValueAt(got, RecordHeaderLength)
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
		record *record
	}{
		{randomRecord(256)},
		{randomRecord(1 << 10)},
		{randomRecord(32 << 10)},
		{randomRecord(64 << 10)},
		{randomRecord(128 << 10)},
	}

	for _, tt := range tc {
		r, w := testReaderWriter(b)
		newEncoder(w).Encode(tt.record) // nolint: errcheck
		dec := newDecoder(r)

		b.Run(fmt.Sprintf("RecordSize: %d, RecordAt", tt.record.size()), func(b *testing.B) {
			record := new(record)
			for b.Loop() {
				b.SetBytes(record.size())
				dec.RecordAt(record, 0) // nolint: errcheck
			}
		})

		b.Run(fmt.Sprintf("RecordSize: %d, ValueAt", tt.record.size()), func(b *testing.B) {
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
		r, w := testReaderWriter(t)
		got := new(record)

		_, err := newEncoder(w).Encode(randomRecord(128))
		AssertNoError(t, err)

		w.(*os.File).Truncate(100)

		_, err = newDecoder(r).RecordAt(got, 0)
		AssertErrorIs(t, err, ErrShortRead)
	})

	t.Run("corrupt record leads to CRC mismatch", func(t *testing.T) {
		r, w := testReaderWriter(t)
		want := randomRecord(128)
		got := new(record)

		_, err := newEncoder(w).Encode(want)
		AssertNoError(t, err).Require()

		// Corrupt the record by writing to the middle of it.
		_, err = w.(*os.File).WriteAt(
			[]byte{byte(255)},
			int64(len(want.Value)-10),
		)
		AssertNoError(t, err).Require()

		_, err = newDecoder(r).RecordAt(got, 0)
		AssertErrorIs(t, err, ErrChecksumMismatch)
	})
}

func TestEncodeDecodeDoesNotAllocate(t *testing.T) {
	r, w := testReaderWriter(t)
	encoder := newEncoder(w)
	decoder := newDecoder(r)

	src := randomRecord(128)
	dst := new(record)

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

func randomRecord(n int64) *record {
	return &record{
		Type:  Type(rand.Uint32()),
		Value: RandomBytes(n),
	}
}
