package raft_test

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/robinkb/cascade/cluster/raft"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	. "github.com/robinkb/cascade/testing"
)

var (
	emptyHardState = raftpb.HardState{}
)

func TestStorageEntries(t *testing.T) {
	entries := index(3).terms(3, 4, 5, 5, 6, 7, 7, 7, 7, 8)
	store := newTestStore(t)
	err := store.Save(entries, emptyHardState, false)
	AssertNoError(t, err)

	tc := []struct {
		name        string
		lo, hi      uint64
		wantEntries []raftpb.Entry
		wantErr     error
	}{
		{"get the first entry",
			3, 4,
			entries[0:1], nil},
		{"get middle entries",
			4, 11,
			entries[1 : len(entries)-2], nil},
		{"get the very last entry",
			11, 12,
			entries[len(entries)-2 : len(entries)-1], nil},
		{"lo before first entry returns ErrCompacted",
			2, 4,
			nil, etcdraft.ErrCompacted},
		// {"hi after"}
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Entries(tt.lo, tt.hi, math.MaxUint64)
			AssertErrorIs(t, err, tt.wantErr)
			AssertDeepEqual(t, got, tt.wantEntries)
		})
	}
}

func TestStorageTerm(t *testing.T) {
	t.Run("for empty storage", func(t *testing.T) {
		store := newTestStore(t)

		fi, _ := store.FirstIndex()
		_, err := store.Term(fi)
		AssertErrorIs(t, err, etcdraft.ErrUnavailable)

		li, _ := store.LastIndex()
		_, err = store.Term(li)
		AssertNoError(t, err)

		_, err = store.Term(li + 1)
		AssertErrorIs(t, err, etcdraft.ErrUnavailable)
	})

	t.Run("for storage with entries", func(t *testing.T) {
		ents := index(3).terms(3, 4, 4, 5)
		store := newTestStore(t)

		err := store.Save(ents, emptyHardState, false)
		AssertNoError(t, err)

		tests := []struct {
			name  string
			i     uint64
			werr  error
			wterm uint64
		}{
			{"index lower than FirstIndex()-1 returns ErrCompacted",
				1, etcdraft.ErrCompacted, 0},
			{"index at FirstIndex() -1 returns term 0",
				2, nil, 0},
			{"first entry returns term 4",
				3, nil, 3},
			{"second entry returns term 4",
				4, nil, 4},
			{"third entry returns term 4",
				5, nil, 4},
			{"fourth entry returns term 5",
				6, nil, 5},
			{"index higher than LastIndex() returns ErrUnavailable",
				7, etcdraft.ErrUnavailable, 0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				term, err := store.Term(tt.i)
				AssertErrorIs(t, err, tt.werr).Require()
				AssertEqual(t, term, tt.wterm).Require()
			})
		}
	})
}

func TestStorageEntries2(t *testing.T) {
	// TODO: Still need to expand my own tests to cover size limiting
	t.SkipNow()

	ents := index(3).terms(3, 4, 5, 6)
	tests := []struct {
		lo, hi, maxsize uint64

		werr     error
		wentries []raftpb.Entry
	}{
		{2, 6, math.MaxUint64, etcdraft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, etcdraft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, index(4).terms(4)},
		{4, 6, math.MaxUint64, nil, index(4).terms(4, 5)},
		{4, 7, math.MaxUint64, nil, index(4).terms(4, 5, 6)},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, index(4).terms(4)},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, index(4).terms(4, 5)},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, index(4).terms(4, 5)},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, index(4).terms(4, 5)},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, index(4).terms(4, 5, 6)},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			store := newTestStore(t)
			err := store.Save(ents, emptyHardState, false)
			AssertNoError(t, err)

			entries, err := store.Entries(tt.lo, tt.hi, tt.maxsize)
			AssertErrorIs(t, err, tt.werr)
			require.Equal(t, tt.wentries, entries)
		})
	}
}

func TestStorageLastIndex(t *testing.T) {
	store := newTestStore(t)

	var want uint64
	got, err := store.LastIndex()
	AssertNoError(t, err).Require()
	AssertEqual(t, got, want)

	entries := index(3).terms(3, 4, want)
	want = entries[len(entries)-1].Index
	err = store.Save(entries, emptyHardState, false)
	AssertNoError(t, err)

	got, err = store.LastIndex()
	AssertNoError(t, err).Require()
	AssertEqual(t, got, want)
}

func TestStorageFirstIndex(t *testing.T) {
	store := newTestStore(t)

	var want uint64
	t.Run("first index of an empty storage is 1", func(t *testing.T) {
		// This reeks like an implementation that had a bug in it.
		// Then the rest of Raft worked around the bug, and now
		// it's here to stay.
		want = 1
		got, err := store.FirstIndex()
		AssertNoError(t, err).Require()
		AssertEqual(t, got, want)
	})

	t.Run("first index of a storage with entries is the index of the first entry", func(t *testing.T) {
		entries := index(want).terms(5, 5, 6, 6, 7, 8)
		want = entries[0].Index
		err := store.Save(entries, emptyHardState, false)
		AssertNoError(t, err)

		got, err := store.FirstIndex()
		AssertNoError(t, err).Require()
		AssertEqual(t, got, want)
	})
}

func TestSetHardState(t *testing.T) {
	store := newTestStore(t)

	want := raftpb.HardState{
		Term:   rand.Uint64(),
		Vote:   rand.Uint64(),
		Commit: rand.Uint64(),
	}

	err := store.Save(nil, want, false)
	AssertNoError(t, err)

	got, _, err := store.InitialState()
	AssertNoError(t, err)
	AssertDeepEqual(t, got, want)
}

func TestApplySnapshot(t *testing.T) {
	store := newTestStore(t)

	want := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: rand.Uint64(),
			Term:  rand.Uint64(),
		},
	}

	err := store.SaveSnapshot(want)
	AssertNoError(t, err)

	got, err := store.Snapshot()
	AssertNoError(t, err)
	AssertDeepEqual(t, got, want)
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	oldDb, err := qwal.Open(dir, nil)
	AssertNoError(t, err).Require()
	oldStore, err := raft.NewDiskStorage(oldDb, new(raft.SpySnapshotter))
	AssertNoError(t, err).Require()

	want := struct {
		hardState raftpb.HardState
		snapshot  raftpb.Snapshot
		entries   []raftpb.Entry
	}{
		hardState: raftpb.HardState{Term: rand.Uint64(), Vote: rand.Uint64()},
		snapshot:  raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{ConfState: raftpb.ConfState{AutoLeave: true}}},
		entries:   index(3).terms(3, 4, 5, 6, 7),
	}

	err = oldStore.Save(want.entries, want.hardState, false)
	AssertNoError(t, err).Require()
	err = oldStore.SaveSnapshot(want.snapshot)
	AssertNoError(t, err).Require()

	newDb, err := qwal.Open(dir, nil)
	AssertNoError(t, err).Require()
	newStore, err := raft.NewDiskStorage(newDb, new(raft.SpySnapshotter))
	AssertNoError(t, err).Require()

	gotHardState, gotConfState, err := newStore.InitialState()
	AssertNoError(t, err).Require()
	AssertDeepEqual(t, gotHardState, want.hardState)
	AssertDeepEqual(t, gotConfState, want.snapshot.Metadata.ConfState)

	lo, err := newStore.FirstIndex()
	AssertNoError(t, err)
	AssertEqual(t, lo, want.entries[0].Index)

	hi, err := newStore.LastIndex()
	AssertNoError(t, err)
	AssertEqual(t, hi, want.entries[len(want.entries)-1].Index)

	gotEntries, err := newStore.Entries(lo, hi+1, math.MaxUint64)
	AssertNoError(t, err)
	AssertDeepEqual(t, gotEntries, want.entries)
}

func TestSnapshot(t *testing.T) {
	db := testDB(t, nil)
	snap := new(raft.SpySnapshotter)
	store, err := raft.NewDiskStorage(db, snap)
	AssertNoError(t, err).Require()

	snapshot, err := store.Snapshot()
	AssertNoError(t, err).Require()
	AssertEqual(t, etcdraft.IsEmptySnap(snapshot), true)

	entries := index(1).terms(1, 1, 1)
	lastApplied := entries[len(entries)-1]
	err = store.Save(entries, emptyHardState, false)
	AssertNoError(t, err).Require()
	// Set the index of the last applied entry. Normally this happens after
	// the entry is applied to the state machine.
	store.AppliedIndex(lastApplied.Index)

	// Cut triggers a snapshot.
	err = db.Cut()
	AssertNoError(t, err).Require()

	// Retrieve the snapshot and do some basic checks to ensure correctness.
	snapshot, err = store.Snapshot()
	AssertNoError(t, err).Require()
	AssertEqual(t, snapshot.Metadata.Index, lastApplied.Index)
	AssertEqual(t, snapshot.Metadata.Term, lastApplied.Term)
	AssertEqual(t, snap.CallStats.Snapshot, 1)
}

func TestCompaction(t *testing.T) {
	db := testDB(t, nil)
	store, err := raft.NewDiskStorage(db, new(raft.SpySnapshotter))
	AssertNoError(t, err).Require()

	oldEntries := index(1).terms(1, 1)
	err = store.Save(oldEntries, emptyHardState, false)
	AssertNoError(t, err)
	store.AppliedIndex(oldEntries[1].Index)

	// Ensure that FirstIndex returns the Index of the Entry that we just put in.
	fi, err := store.FirstIndex()
	AssertNoError(t, err)
	AssertEqual(t, fi, oldEntries[0].Index)

	// We should be able to retrieve our Entry.
	got1, err := store.Entries(1, 2, math.MaxUint64)
	AssertNoError(t, err)
	AssertDeepEqual(t, got1[0], oldEntries[0])

	// Cut a new Log.
	err = db.Cut()
	AssertNoError(t, err).Require()

	// Save a new Entry.
	newEntries := index(3).terms(2, 2, 2)
	err = store.Save(newEntries, emptyHardState, false)
	AssertNoError(t, err).Require()
	store.AppliedIndex(newEntries[2].Index)

	// Compact, which should remove the first Log containing the first Entry.
	err = db.Compact()
	AssertNoError(t, err).Require()

	// FirstIndex should now return the Index of our new Entry.
	fi, err = store.FirstIndex()
	AssertNoError(t, err)
	AssertEqual(t, fi, newEntries[0].Index)

	// LastIndex returns the index of the last new entry.
	li, err := store.LastIndex()
	AssertNoError(t, err)
	AssertEqual(t, li, newEntries[len(newEntries)-1].Index)

	// The Entry that was pushed earlier should be unavailable.
	_, err = store.Entries(1, 2, math.MaxUint64)
	AssertErrorIs(t, err, etcdraft.ErrCompacted)

	// The new Entry should also be available.
	got2, err := store.Entries(3, 4, math.MaxUint64)
	AssertNoError(t, err).Require()
	AssertDeepEqual(t, got2[0], newEntries[0])

	// Term of the last compacted entry should still be available as well.
	term, err := store.Term(fi - 1)
	AssertNoError(t, err)
	AssertEqual(t, term, oldEntries[0].Index)
}

// index is a helper type for generating slices of raftpb.Entry. The value of index
// is the first entry index in the generated slices.
type index uint64

// terms generates a slice of entries at indices [index, index+len(terms)), with
// the given terms of each entry. Terms must be non-decreasing.
func (i index) terms(terms ...uint64) []raftpb.Entry {
	index := uint64(i)
	entries := make([]raftpb.Entry, 0, len(terms))
	for _, term := range terms {
		entries = append(entries, raftpb.Entry{Term: term, Index: index})
		index++
	}
	return entries
}

func testDB(t *testing.T, opts *qwal.Options) qwal.DB {
	dir := t.TempDir()
	db, err := qwal.Open(dir, opts)
	AssertNoError(t, err).Require()

	return db
}

func newTestStore(t *testing.T) *raft.DiskStorage {
	store, err := raft.NewDiskStorage(testDB(t, nil), new(raft.SpySnapshotter))
	AssertNoError(t, err).Require()
	return store
}
