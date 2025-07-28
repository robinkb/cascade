package logdeck

import (
	"math/rand/v2"
	"os"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestDBAppend(t *testing.T) {
	db := testDB(t, nil)

	wantType := randomType()
	wantValues := RandomBytesN(5, 16, 32)

	count := db.Count(wantType)
	AssertEqual(t, count, 0)
	_, err := db.First(wantType)
	AssertErrorIs(t, err, ErrTypeUnknown)
	_, err = db.Last(wantType)
	AssertErrorIs(t, err, ErrTypeUnknown)

	for i, val := range wantValues {
		err := db.Append(wantType, val)
		AssertNoError(t, err).Require()

		// Make sure that Count and Last account for the new value,
		// and that First has not changed.
		count := db.Count(wantType)
		AssertEqual(t, count, i+1)
		got, err := db.First(wantType)
		AssertSlicesEqual(t, got, wantValues[0])
		got, err = db.Last(wantType)
		AssertSlicesEqual(t, got, val)
	}
}

func TestDBGet(t *testing.T) {
	db := testDB(t, nil)

	wantType, wantValue := randomType(), RandomBytes(32)

	err := db.Append(wantType, wantValue)
	AssertNoError(t, err).Require()

	t.Run("Get returns val", func(t *testing.T) {
		gotVal, err := db.Get(wantType, 0)
		AssertNoError(t, err)
		AssertSlicesEqual(t, gotVal, wantValue)
	})

	t.Run("Get with unknown type returns ErrTypeUnknown", func(t *testing.T) {
		_, err := db.Get(0, 0)
		AssertErrorIs(t, err, ErrTypeUnknown)
	})

	t.Run("Get out of bounds returns ErrIndexOutOfBounds", func(t *testing.T) {
		_, err := db.Get(wantType, -1)
		AssertErrorIs(t, err, ErrIndexOutOfBounds)
	})
}

func testDB(t *testing.T, opts *Options) DB {
	dir := t.TempDir()
	t.Cleanup(func() {
		os.RemoveAll(dir) // nolint: errcheck
	})

	deck, err := Open(dir, opts)
	AssertNoError(t, err).Require()

	return deck
}

func randomRecordsN(n int, minSize, maxSize int64) []*record {
	records := make([]*record, n)
	for i := range records {
		records[i] = randomRecord(rand.Int64N(maxSize-minSize) + minSize)
	}
	return records
}
