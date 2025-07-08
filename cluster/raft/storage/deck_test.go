package storage_test

import (
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestNoSé(t *testing.T) {
	want := randomRecordsN(10, 150, 200)
	d := storage.NewDeck(t.TempDir(), &storage.DeckConfig{
		MaxLogSize:  256,
		MaxLogCount: 5,
	})

	compactions := make([]int64, 0)

	go func() {
		for c := range d.Compactions() {
			compactions = append(compactions, c)
		}
	}()

	for i := range want {
		err := d.Append(want[i])
		AssertNoError(t, err).Require()
	}

	i := 0
	for log := range d.All() {
		for record := range log.All() {
			AssertStructsEqual(t, record, want[i])
			i++
		}
	}

	// The number 4 was manually verified, ngl.
	AssertEqual(t, len(compactions), 4)
}

func TestDeckInNewDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), RandomString(8))
	d := storage.NewDeck(path, nil)

	// Write whatever to force Deck to create a file.
	err := d.Append(randomRecord(128))
	AssertNoError(t, err)
}

func randomRecordsN(n int, minSize, maxSize int64) []*storage.Record {
	records := make([]*storage.Record, n)
	for i := range records {
		records[i] = randomRecord(rand.Int64N(maxSize-minSize) + minSize)
	}
	return records
}
