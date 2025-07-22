package logdeck_test

import (
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/logdeck"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestNoSé(t *testing.T) {
	want := RandomContentsN(10, 150, 200)
	d := logdeck.Open(t.TempDir(), &logdeck.Options{
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
	d := logdeck.Open(path, nil)

	// Write whatever to force Deck to create a file.
	err := d.Append(randomRecordType(), RandomContents(128))
	AssertNoError(t, err)
}

func randomRecordsN(n int, minSize, maxSize int64) []*logdeck.Record {
	records := make([]*logdeck.Record, n)
	for i := range records {
		records[i] = randomRecord(rand.Int64N(maxSize-minSize) + minSize)
	}
	return records
}
