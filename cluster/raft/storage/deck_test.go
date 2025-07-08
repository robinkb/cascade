package storage_test

import (
	"math/rand/v2"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestNoSé(t *testing.T) {
	want := randomRecordsN(100, 127, 128)
	d := storage.NewDeck(t.TempDir(), &storage.DeckConfig{
		MaxLogSize:  256,
		MaxLogCount: 10,
	})

	compactions := make([]int64, 0)
	defer t.Log("compactions:", compactions)

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
}

func randomRecordsN(n int, minSize, maxSize int64) []*storage.Record {
	records := make([]*storage.Record, n)
	for i := range records {
		records[i] = randomRecord(rand.Int64N(maxSize-minSize) + minSize)
	}
	return records
}
