package logdeck

import (
	"errors"
	"math/rand/v2"
	"os"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestDBAppend(t *testing.T) {
	t.Run("Appended values are retrievable", func(t *testing.T) {
		db := testDB(t, nil)

		wantType := randomType()
		wantValues := RandomBytesN(5, 16, 32)

		count := db.Count(wantType)
		AssertEqual(t, count, 0)
		_, err := db.First(wantType)
		AssertErrorIs(t, err, ErrTypeUnknown)
		_, err = db.Last(wantType)
		AssertErrorIs(t, err, ErrTypeUnknown)

		for _, val := range wantValues {
			err := db.Append(wantType, val)
			AssertNoError(t, err).Require()
		}

		count = db.Count(wantType)
		AssertEqual(t, count, len(wantValues))
		got, err := db.First(wantType)
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, wantValues[0])
		got, err = db.Last(wantType)
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, wantValues[len(wantValues)-1])
	})

	t.Run("Append triggers Cut when MaxLogSize exceeded", func(t *testing.T) {
		db := testDB(t, &Options{
			MaxLogSize: 64,
		})

		cuts := 0
		db.CutHook(func(id LogID) error {
			cuts++
			return nil
		})

		err := db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		err = db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)

		AssertEqual(t, cuts, 1)
	})

	t.Run("Append triggers Cut when MaxLogRecordCount exceeded", func(t *testing.T) {
		db := testDB(t, &Options{
			MaxLogRecordCount: 1,
		})

		cuts := 0
		db.CutHook(func(id LogID) error {
			cuts++
			return nil
		})

		err := db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		err = db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)

		AssertEqual(t, cuts, 1)
	})

	t.Run("Append triggers Compact when MaxLogCount exceeded", func(t *testing.T) {
		db := testDB(t, &Options{
			MaxLogRecordCount: 1,
			MaxLogCount:       1,
		})

		compacts := 0
		db.CompactHook(func(c Counters) error {
			compacts++
			return nil
		})

		err := db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		err = db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)

		AssertEqual(t, compacts, 1)
	})
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

func TestDBCut(t *testing.T) {
	t.Run("Cut provisions a new Log every time it is called", func(t *testing.T) {
		db := testDB(t, nil).(*db)

		target := 5

		// Start at 2 because a DB starts with 1 Log.
		for want := 2; want < target; want++ {
			err := db.Cut()
			AssertNoError(t, err)

			got := len(db.logs)
			AssertEqual(t, got, want)
		}
	})

	t.Run("Cut calls CutHook", func(t *testing.T) {
		db := testDB(t, nil)

		want := true
		got := false
		db.CutHook(func(id LogID) error {
			got = true
			return nil
		})

		err := db.Cut()
		AssertNoError(t, err)
		AssertEqual(t, got, want)
	})

	t.Run("LogID is passed to CutHook and incremented correctly", func(t *testing.T) {
		db := testDB(t, nil)

		var got LogID
		db.CutHook(func(id LogID) error {
			got = id
			return nil
		})

		for want := range 5 {
			err := db.Cut()
			AssertNoError(t, err)
			AssertEqual(t, got, LogID(want))
		}
	})

	t.Run("Error from CutHook is bubbled up to Cut", func(t *testing.T) {
		db := testDB(t, nil)

		want := errors.New("error when calling CutHook")
		db.CutHook(func(id LogID) error {
			return want
		})

		got := db.Cut()
		AssertErrorIs(t, got, want)
		AssertErrorIs(t, got, ErrCutHookFailed)
	})
}

func TestDBCompact(t *testing.T) {
	t.Run("Compact removes a Log", func(t *testing.T) {
		db := testDB(t, nil).(*db)

		// Verify that we start with one Log.
		got := len(db.logs)
		AssertEqual(t, got, 1)

		err := db.Cut()
		AssertNoError(t, err)

		// Verify that we now have two.
		got = len(db.logs)
		AssertEqual(t, got, 2)

		// Compaction should bring that back to one.
		err = db.Compact()
		AssertNoError(t, err)

		got = len(db.logs)
		AssertEqual(t, got, 1)
	})

	t.Run("Compact when DB contains just one Log returns ErrInvalidCompaction", func(t *testing.T) {
		db := testDB(t, nil)

		err := db.Compact()
		AssertErrorIs(t, err, ErrInvalidCompaction)
	})

	t.Run("Compact calls CompactHook", func(t *testing.T) {
		db := testDB(t, nil)

		called := false
		db.CompactHook(func(c Counters) error {
			called = true
			return nil
		})

		err := db.Cut()
		AssertNoError(t, err)

		err = db.Compact()
		AssertNoError(t, err)

		AssertEqual(t, called, true)
	})

	t.Run("Error in CompactHook is bubbled up to Compact", func(t *testing.T) {
		db := testDB(t, nil)

		want := errors.New("error when calling CompactHook")
		db.CompactHook(func(c Counters) error {
			return want
		})

		err := db.Cut()
		AssertNoError(t, err)

		got := db.Compact()
		AssertErrorIs(t, got, want)
		AssertErrorIs(t, got, ErrCompactHookFailed)
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
