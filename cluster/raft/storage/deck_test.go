package storage_test

import (
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestNoSé(t *testing.T) {
	want := RandomContentsN(10, 150, 200)
	d := storage.NewDeck(t.TempDir(), &storage.DeckConfig{
		MaxLogSize:  256,
		MaxLogCount: 5,
	})

	for i := range want {
		err := d.Append(randomRecordType(), want[i])
		AssertNoError(t, err).Require()
	}
}

func TestDeckInNewDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), RandomString(8))
	d := storage.NewDeck(path, nil)

	// Write whatever to force Deck to create a file.
	err := d.Append(randomRecordType(), RandomContents(128))
	AssertNoError(t, err)
}

/*
*
Results on 20/07/2025 at commit 53877d9608fc8a

goos: linux
goarch: amd64
pkg: github.com/robinkb/cascade-registry/cluster/raft/storage
cpu: AMD Ryzen 7 7840U w/ Radeon  780M Graphics
BenchmarkDeckAppend/RecordSize:1kB_Sync:false-16         	  264784	      4462 ns/op	 233.07 MB/s	     247 B/op	       1 allocs/op
BenchmarkDeckAppend/RecordSize:32kB_Sync:false-16        	   20526	     57973 ns/op	 565.50 MB/s	     238 B/op	       1 allocs/op
BenchmarkDeckAppend/RecordSize:64kB_Sync:false-16        	    9496	    114859 ns/op	 570.72 MB/s	     305 B/op	       1 allocs/op
BenchmarkDeckAppend/RecordSize:128kB_Sync:false-16       	    5356	    223121 ns/op	 587.52 MB/s	     499 B/op	       1 allocs/op
BenchmarkDeckAppend/RecordSize:1kB_Sync:true-16          	  250089	      4660 ns/op	 223.20 MB/s	     252 B/op	       1 allocs/op
BenchmarkDeckAppend/RecordSize:32kB_Sync:true-16         	   17704	     58852 ns/op	 557.06 MB/s	     256 B/op	       1 allocs/op
BenchmarkDeckAppend/RecordSize:64kB_Sync:true-16         	   10497	    113176 ns/op	 579.20 MB/s	     291 B/op	       1 allocs/op
BenchmarkDeckAppend/RecordSize:128kB_Sync:true-16        	    5031	    226418 ns/op	 578.96 MB/s	     495 B/op	       1 allocs/op
PASS
*/
func BenchmarkDeckAppend(b *testing.B) {
	tc := []struct {
		size int
		sync bool
	}{
		{1 << 10, false},
		{32 << 10, false},
		{64 << 10, false},
		{128 << 10, false},
		{1 << 10, true},
		{32 << 10, true},
		{64 << 10, true},
		{128 << 10, true},
	}

	for _, tt := range tc {
		name := fmt.Sprintf("RecordSize:%dkB Sync:%t", tt.size/1024, tt.sync)
		b.Run(name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), RandomString(8))
			deck := storage.NewDeck(path, nil)

			value := make([]byte, tt.size)

			for b.Loop() {
				b.SetBytes(int64(tt.size) + storage.RecordHeaderLength)

				t := storage.RecordType(rand.Uint64())
				crand.Read(value) // nolint: errcheck

				deck.Append(t, value) // nolint: errcheck
				if tt.sync {
					deck.Sync() // nolint: errcheck
				}
			}
		})
	}
}

func randomRecordsN(n int, minSize, maxSize int64) []*storage.Record {
	records := make([]*storage.Record, n)
	for i := range records {
		records[i] = randomRecord(rand.Int64N(maxSize-minSize) + minSize)
	}
	return records
}
