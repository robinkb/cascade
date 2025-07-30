package raft

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/robinkb/cascade-registry/cluster/raft/logdeck"
	. "github.com/robinkb/cascade-registry/testing"
)

var (
	emptyHardState = raftpb.HardState{}
)

func TestStorageEntries(t *testing.T) {
	entries := index(3).terms(3, 4, 5, 5, 6, 7, 7, 7, 7, 8)
	l, err := NewDiskStorage(t.TempDir(), new(SpySnapshotter), nil)
	AssertNoError(t, err).Require()
	err = l.Save(entries, emptyHardState, false)
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
			nil, raft.ErrCompacted},
		// {"hi after"}
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			got, err := l.Entries(tt.lo, tt.hi, math.MaxUint64)
			AssertErrorIs(t, err, tt.wantErr)
			AssertDeepEqual(t, got, tt.wantEntries)
		})
	}
}

func TestStorageTerm(t *testing.T) {
	t.Run("for empty storage", func(t *testing.T) {
		l, err := NewDiskStorage(t.TempDir(), nil, nil)
		AssertNoError(t, err).Require()

		fi, _ := l.FirstIndex()
		_, err = l.Term(fi)
		AssertErrorIs(t, err, raft.ErrUnavailable)

		li, _ := l.LastIndex()
		_, err = l.Term(li)
		AssertNoError(t, err)

		_, err = l.Term(li + 1)
		AssertErrorIs(t, err, raft.ErrUnavailable)
	})

	t.Run("for storage with entries", func(t *testing.T) {
		ents := index(3).terms(3, 4, 4, 5)

		l, err := NewDiskStorage(t.TempDir(), nil, nil)
		AssertNoError(t, err).Require()
		err = l.Save(ents, emptyHardState, false)
		AssertNoError(t, err)

		tests := []struct {
			name  string
			i     uint64
			werr  error
			wterm uint64
		}{
			{"index lower than FirstIndex()-1 returns ErrCompacted",
				1, raft.ErrCompacted, 0},
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
				7, raft.ErrUnavailable, 0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				term, err := l.Term(tt.i)
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
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
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
			l, err := NewDiskStorage(t.TempDir(), nil, nil)
			AssertNoError(t, err).Require()
			err = l.Save(ents, emptyHardState, false)
			AssertNoError(t, err)

			entries, err := l.Entries(tt.lo, tt.hi, tt.maxsize)
			AssertErrorIs(t, err, tt.werr)
			require.Equal(t, tt.wentries, entries)
		})
	}
}

func TestStorageLastIndex(t *testing.T) {
	l, err := NewDiskStorage(t.TempDir(), nil, nil)
	AssertNoError(t, err).Require()

	var want uint64
	got, err := l.LastIndex()
	AssertNoError(t, err).Require()
	AssertEqual(t, got, want)

	entries := index(3).terms(3, 4, want)
	want = entries[len(entries)-1].Index
	err = l.Save(entries, emptyHardState, false)
	AssertNoError(t, err)

	got, err = l.LastIndex()
	AssertNoError(t, err).Require()
	AssertEqual(t, got, want)
}

func TestStorageFirstIndex(t *testing.T) {
	l, err := NewDiskStorage(t.TempDir(), nil, nil)
	AssertNoError(t, err).Require()
	var want uint64

	t.Run("first index of an empty storage is 1", func(t *testing.T) {
		// This reeks like an implementation that had a bug in it.
		// Then the rest of Raft worked around the bug, and now
		// it's here to stay.
		want = 1
		got, err := l.FirstIndex()
		AssertNoError(t, err).Require()
		AssertEqual(t, got, want)
	})

	t.Run("first index of a storage with entries is the index of the first entry", func(t *testing.T) {
		entries := index(want).terms(5, 5, 6, 6, 7, 8)
		want = entries[0].Index
		err := l.Save(entries, emptyHardState, false)
		AssertNoError(t, err)

		got, err := l.FirstIndex()
		AssertNoError(t, err).Require()
		AssertEqual(t, got, want)
	})
}

func TestSetHardState(t *testing.T) {
	l, err := NewDiskStorage(t.TempDir(), nil, nil)
	AssertNoError(t, err).Require()

	want := raftpb.HardState{
		Term:   rand.Uint64(),
		Vote:   rand.Uint64(),
		Commit: rand.Uint64(),
	}

	err = l.Save(nil, want, false)
	AssertNoError(t, err)

	got, _, err := l.InitialState()
	AssertNoError(t, err)
	AssertDeepEqual(t, got, want)
}

func TestApplySnapshot(t *testing.T) {
	l, err := NewDiskStorage(t.TempDir(), nil, nil)
	AssertNoError(t, err).Require()

	want := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: rand.Uint64(),
			Term:  rand.Uint64(),
		},
	}

	err = l.SaveSnapshot(want)
	AssertNoError(t, err)

	got, err := l.Snapshot()
	AssertNoError(t, err)
	AssertDeepEqual(t, got, want)
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	oldLog, err := NewDiskStorage(dir, nil, nil)
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

	err = oldLog.Save(want.entries, want.hardState, false)
	AssertNoError(t, err).Require()
	err = oldLog.SaveSnapshot(want.snapshot)
	AssertNoError(t, err).Require()

	newLog, err := NewDiskStorage(dir, nil, nil)
	AssertNoError(t, err).Require()

	gotHardState, gotConfState, err := newLog.InitialState()
	AssertNoError(t, err).Require()
	AssertDeepEqual(t, gotHardState, want.hardState)
	AssertDeepEqual(t, gotConfState, want.snapshot.Metadata.ConfState)

	lo, err := newLog.FirstIndex()
	AssertNoError(t, err)
	AssertEqual(t, lo, want.entries[0].Index)

	hi, err := newLog.LastIndex()
	AssertNoError(t, err)
	AssertEqual(t, hi, want.entries[len(want.entries)-1].Index)

	gotEntries, err := newLog.Entries(lo, hi+1, math.MaxUint64)
	AssertNoError(t, err)
	AssertDeepEqual(t, gotEntries, want.entries)
}

// TestCompaction is probably a bit too big, and asserts a little too much.
// Basically everything to do with compaction.
// And it broke after adding the CutHandler. Definitely needs to be reworked.
func TestCompaction(t *testing.T) {
	t.SkipNow()
	// Prepare a store with a ridiculously low limit
	// to immediately trigger compactions.
	store, err := NewDiskStorage(t.TempDir(), new(SpySnapshotter), &logdeck.Options{
		MaxLogSize:  64,
		MaxLogCount: 1,
	})
	AssertNoError(t, err).Require()

	// This entry will "fill up" the first Log.
	want1 := index(1).terms(1)
	// Not really for testing, but to ensure that the size is as expected.
	// With MaxLogSize of 64, only one entry can fit in a log.
	// And with MaxLogCount of 1, a second entry will immediately trigger a compaction,
	// and push the first entry out.
	AssertEqual(t, logdeck.RecordHeaderLength+want1[0].Size(), 22)
	err = store.Save(want1, emptyHardState, false)
	AssertNoError(t, err)

	// Ensure that FirstIndex returns the Index of the Entry that we just put in.
	fi, err := store.FirstIndex()
	AssertNoError(t, err)
	AssertEqual(t, fi, want1[0].Index)

	// We should be able to retrieve our Entry.
	got1, err := store.Entries(1, 2, math.MaxUint64)
	AssertNoError(t, err)
	AssertDeepEqual(t, got1[0], want1[0])

	// This second Entry should push our little store over its limit
	// and cause the first Log containing the first Entry to be compacted.
	want2 := index(2).terms(2)
	err = store.Save(want2, emptyHardState, false)
	AssertNoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// FirstIndex should now return the Index of our new Entry.
	fi, err = store.FirstIndex()
	AssertNoError(t, err)
	AssertEqual(t, fi, want2[0].Index)
	// The Entry that was pushed earlier should be unavailable.
	_, err = store.Entries(1, 2, math.MaxUint64)
	AssertErrorIs(t, err, raft.ErrCompacted)
	// The new Entry should also be available.
	got2, err := store.Entries(2, 3, math.MaxUint64)
	AssertNoError(t, err)
	AssertDeepEqual(t, got2[0], want2[0])
	// Term of the last compacted entry should still be available as well.
	// TODO: This is only correct when compacting out one entry. So in reality, never.
	term, err := store.Term(fi - 1)
	AssertNoError(t, err)
	AssertEqual(t, term, want1[0].Index)
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
