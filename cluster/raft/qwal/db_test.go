package qwal

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	. "github.com/robinkb/cascade/testing"
)

func TestDBAppend(t *testing.T) {
	t.Run("Appended values are retrievable", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), nil)

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
		AssertEqual(t, count, uint64(len(wantValues)))
		got, err := db.First(wantType)
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, wantValues[0])
		got, err = db.Last(wantType)
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, wantValues[len(wantValues)-1])
	})

	t.Run("Append triggers Cut when MaxLogSize exceeded", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), &Options{
			MaxLogSize: 64,
		})

		cuts := 0
		db.CutHook(func(id LogID) error {
			cuts++
			return nil
		})

		err := db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		AssertEqual(t, cuts, 0)

		err = db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		AssertEqual(t, cuts, 1)
	})

	t.Run("Append triggers Cut when MaxLogValueCount exceeded", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), &Options{
			MaxLogValueCount: 1,
		})

		cuts := 0
		db.CutHook(func(id LogID) error {
			cuts++
			return nil
		})

		err := db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		AssertEqual(t, cuts, 0)

		err = db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		AssertEqual(t, cuts, 1)
	})

	t.Run("Append triggers Compact when MaxLogCount exceeded", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), &Options{
			MaxLogValueCount: 1,
			MaxLogCount:      1,
		})

		compacts := 0
		db.CompactHook(func(counts Counters) error {
			compacts++
			return nil
		})

		err := db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		AssertEqual(t, compacts, 0)

		err = db.Append(randomType(), RandomBytes(32))
		AssertNoError(t, err)
		AssertEqual(t, compacts, 1)
	})
}

func TestDBGet(t *testing.T) {
	db := testReplayedDB(t, t.TempDir(), nil)

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
		_, err := db.Get(wantType, 100)
		AssertErrorIs(t, err, ErrIndexOutOfBounds)
	})
}

func TestDBCut(t *testing.T) {
	t.Run("Cut provisions a new Log every time it is called", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), nil)

		target := 5

		// Start at 2 because a DB starts with 1 Log.
		for want := 2; want < target; want++ {
			err := db.Cut()
			AssertNoError(t, err)
			AssertEqual(t, db.Status().LogCount, want)
		}
	})

	t.Run("Cut calls CutHook", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), nil)

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
		db := testReplayedDB(t, t.TempDir(), nil)

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
		db := testReplayedDB(t, t.TempDir(), nil)

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
		db := testReplayedDB(t, t.TempDir(), nil)

		// Verify that we start with one Log.
		AssertEqual(t, db.Status().LogCount, 1)

		err := db.Cut()
		AssertNoError(t, err)

		// Verify that we now have two.
		AssertEqual(t, db.Status().LogCount, 2)

		// Compaction should bring that back to one.
		err = db.Compact()
		AssertNoError(t, err)

		AssertEqual(t, db.Status().LogCount, 1)
	})

	t.Run("Compact when DB contains just one Log returns ErrInvalidCompaction", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), nil)

		err := db.Compact()
		AssertErrorIs(t, err, ErrInvalidCompaction)
	})

	t.Run("Compact calls CompactHook", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), nil)

		called := false
		db.CompactHook(func(counts Counters) error {
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
		db := testReplayedDB(t, t.TempDir(), nil)

		want := errors.New("error when calling CompactHook")
		db.CompactHook(func(counts Counters) error {
			return want
		})

		err := db.Cut()
		AssertNoError(t, err)

		got := db.Compact()
		AssertErrorIs(t, got, want)
		AssertErrorIs(t, got, ErrCompactHookFailed)
	})
}

func TestDBDiscard(t *testing.T) {
	t.Run("Discard removes all but the active log", func(t *testing.T) {
		db := testReplayedDB(t, t.TempDir(), nil)

		rType := randomType()
		count := 5
		for range count {
			err := db.Cut()
			AssertNoError(t, err).Require()
			err = db.Append(rType, RandomBytes(32))
			AssertNoError(t, err).Require()
		}

		AssertEqual(t, db.Status().LogCount, count+1)

		err := db.Discard()
		AssertNoError(t, err)
		AssertEqual(t, db.Status().LogCount, 1)
		AssertEqual(t, db.Count(rType), 1)
	})
}

func TestDBOpen(t *testing.T) {
	t.Run("returns an error when the path is not a directory", func(t *testing.T) {
		dir := t.TempDir()
		notadir := filepath.Join(dir, "oops")
		err := os.WriteFile(notadir, RandomBytes(2), os.ModeAppend)
		AssertNoError(t, err)

		_, err = Open(notadir, nil)
		AssertErrorIs(t, err, ErrNotDirectory)
	})
}

func TestDBReplay(t *testing.T) {
	t.Run("re-open db and write data", func(t *testing.T) {
		dir := t.TempDir()
		db := testReplayedDB(t, dir, nil)

		// Generate some random values to write.
		// Half will be written now, and half after recovery.
		wantType := randomType()
		wantValues := RandomBytesN(8, 1<<10, 10<<10)

		// Write the first half.
		for i := 0; i < len(wantValues)/2; i++ {
			err := db.Append(wantType, wantValues[i])
			AssertNoError(t, err).Require()
		}

		err := db.Close()
		AssertNoError(t, err).Require()

		// Re-open DB and rebuild state from directory.
		db = testReplayedDB(t, dir, nil)

		// Write the remaining values
		for i := len(wantValues) / 2; i < len(wantValues); i++ {
			err := db.Append(wantType, wantValues[i])
			AssertNoError(t, err).Require()
		}

		// Make sure that all are available.
		for i, wantValue := range wantValues {
			got, err := db.Get(wantType, uint64(i))
			AssertNoError(t, err)
			AssertSlicesEqual(t, got, wantValue)
		}
	})

	t.Run("recover after cuts and compactions", func(t *testing.T) {
		dir := t.TempDir()
		oldDB := testReplayedDB(t, dir, &Options{
			MaxLogCount: 8,
		})

		// Generate a bunch of random values, appending each to the DB.
		// We call Cut after every Append, because we want to create
		// a lot of Log files. We also need to trigger at least a few
		// compactions, so we should append more values than MaxLogCount.
		wantType := randomType()
		wantValues := RandomBytesN(16, 1<<10, 10<<10)
		for _, value := range wantValues {
			err := oldDB.Append(wantType, value)
			AssertNoError(t, err).Require()
			err = oldDB.Cut()
			AssertNoError(t, err).Require()
		}

		// The amount of values left in DB after compaction.
		remainingValueCount := oldDB.Count(wantType)

		err := oldDB.Close()
		AssertNoError(t, err).Require()

		// Open a new DB in the same directory.
		newDB := testReplayedDB(t, dir, nil).(*db)

		// Check all remaining values
		for i := range remainingValueCount {
			got, err := newDB.Get(wantType, i)
			AssertNoError(t, err)
			AssertSlicesEqual(t, got, wantValues[remainingValueCount+i])
		}

		// Sequence should be restored to last log's ID
		lastLog := newDB.activeLog()
		AssertEqual(t, newDB.sequence, uint64(lastLog.ID))
	})

	t.Run("detects a missing log", func(t *testing.T) {
		dir := t.TempDir()
		db := testReplayedDB(t, dir, &Options{
			MaxLogCount: 3,
		})

		wantLogsCreated := 5
		for range wantLogsCreated {
			err := db.Append(Type(0), RandomBytes(32))
			AssertNoError(t, err).Require()
			err = db.Cut()
			AssertNoError(t, err).Require()
		}

		err := db.Close()
		AssertNoError(t, err).Require()

		// At this point we have logs with sequences 2, 3, 4, and 5.
		// Delete log file with sequence 3.
		logFileName := filepath.Join(dir, fmt.Sprintf(logNameFmt, 3))
		err = os.Remove(logFileName)
		AssertNoError(t, err).Require()

		db = testDB(t, dir, nil)
		err = db.Replay()
		AssertErrorIs(t, err, ErrMissingLogFile)
	})
}

func TestDBReplayHook(t *testing.T) {
	// These subtests only do reading on this DB.
	// Modifying it could break other tests.
	dir := t.TempDir()
	db := testDB(t, dir, nil)
	err := db.Replay()
	AssertNoError(t, err).Require()

	records := randomRecordsN(10, 32, 64)
	for _, record := range records {
		err := db.Append(record.Type, record.Value)
		AssertNoError(t, err).Require()
	}

	err = db.Close()
	AssertNoError(t, err).Require()

	t.Run("iterates over records in the DB", func(t *testing.T) {
		db := testDB(t, dir, nil)

		i := 0
		db.ReplayHook(func(rt Type, value []byte) error {
			AssertEqual(t, rt, records[i].Type)
			AssertSlicesEqual(t, value, records[i].Value)
			i++
			return nil
		})

		err = db.Replay()
		AssertNoError(t, err)
		AssertEqual(t, i, len(records))
	})

	t.Run("returns error in ReplayHook", func(t *testing.T) {
		db := testDB(t, dir, nil)
		want := errors.New("oops")

		db.ReplayHook(func(t Type, value []byte) error {
			return want
		})

		err := db.Replay()
		AssertErrorIs(t, err, ErrReplayHookFailed, want)
	})

	t.Run("can replay without a hook registered", func(t *testing.T) {
		db := testDB(t, dir, nil)
		err := db.Replay()
		AssertNoError(t, err)
	})

	t.Run("replaying a second time does nothing", func(t *testing.T) {
		// Open and replay.
		db := testDB(t, dir, nil)
		err := db.Replay()
		AssertNoError(t, err)

		// Put a value in it.
		err = db.Append(Type(0), RandomBytes(42))
		AssertNoError(t, err)

		// Replay a second time.
		err = db.Replay()
		AssertNoError(t, err)

		// Should still have just one value.
		count := db.Count(Type(0))
		AssertEqual(t, count, 1)
	})

	t.Run("reading or writing panics before replaying", func(t *testing.T) {
		db := testDB(t, dir, nil)

		tc := []struct {
			name   string
			method func(db DB) error
		}{
			{name: "Append", method: func(db DB) error {
				return db.Append(1, nil)
			}},
			{name: "Get", method: func(db DB) error {
				_, err := db.Get(0, 0)
				return err
			}},
			{name: "Count", method: func(db DB) error {
				db.Count(0)
				return nil
			}},
			{name: "First", method: func(db DB) error {
				_, err := db.First(0)
				return err
			}},
			{name: "Last", method: func(db DB) error {
				_, err := db.Last(0)
				return err
			}},
			{name: "Range", method: func(db DB) error {
				db.Range(0, 0, 0)
				return err
			}},
			{name: "Cut", method: func(db DB) error {
				return db.Cut()
			}},
			{name: "Compact", method: func(db DB) error {
				return db.Compact()
			}},
			{name: "Discard", method: func(db DB) error {
				return db.Discard()
			}},
			{name: "Sync", method: func(db DB) error {
				return db.Sync()
			}},
			{name: "Close", method: func(db DB) error {
				return db.Close()
			}},
		}

		for _, tt := range tc {
			name := fmt.Sprintf("method: %s", tt.name)
			t.Run(name, func(t *testing.T) {
				defer AssertPanics(t, ErrMustReplay)
				err := tt.method(db)
				AssertNoError(t, err)
			})
		}
	})
}

func testDB(t *testing.T, dir string, opts *Options) DB {
	db, err := Open(dir, opts)
	AssertNoError(t, err).Require()
	return db
}

func testReplayedDB(t *testing.T, dir string, opts *Options) DB {
	db := testDB(t, dir, opts)
	err := db.Replay()
	AssertNoError(t, err).Require()
	return db
}

func randomRecordsN(n int, minSize, maxSize int64) []*record {
	records := make([]*record, n)
	for i := range records {
		records[i] = randomRecord(rand.Int64N(maxSize-minSize) + minSize)
	}
	return records
}
