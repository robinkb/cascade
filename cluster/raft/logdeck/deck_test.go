package logdeck_test

import (
	"math/rand/v2"
	"os"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/logdeck"
	. "github.com/robinkb/cascade-registry/testing"
)

func testDB(t *testing.T, opts *logdeck.Options) logdeck.DB {
	dir := t.TempDir()
	t.Cleanup(func() {
		os.RemoveAll(dir) // nolint: errcheck
	})

	deck, err := logdeck.Open(dir, opts)
	AssertNoError(t, err).Require()

	return deck
}

func randomRecordsN(n int, minSize, maxSize int64) []*logdeck.Record {
	records := make([]*logdeck.Record, n)
	for i := range records {
		records[i] = randomRecord(rand.Int64N(maxSize-minSize) + minSize)
	}
	return records
}
