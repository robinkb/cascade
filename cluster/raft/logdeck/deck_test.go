package logdeck

import (
	"math/rand/v2"
	"os"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func testDB(t *testing.T, opts *Options) DB {
	dir := t.TempDir()
	t.Cleanup(func() {
		os.RemoveAll(dir) // nolint: errcheck
	})

	deck, err := Open(dir, opts)
	AssertNoError(t, err).Require()

	return deck
}

func randomRecordsN(n int, minSize, maxSize int64) []*Record {
	records := make([]*Record, n)
	for i := range records {
		records[i] = randomRecord(rand.Int64N(maxSize-minSize) + minSize)
	}
	return records
}
