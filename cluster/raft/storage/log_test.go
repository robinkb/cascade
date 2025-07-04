package storage_test

import (
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func tempLog(t *testing.T) (io.ReaderAt, io.Writer) {
	filename := filepath.Join(t.TempDir(), "log.bin")
	w, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	AssertNoError(t, err).Require()

	r, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	AssertNoError(t, err).Require()

	t.Cleanup(func() {
		r.Close()
		w.Close()
	})

	return r, w
}

func TestStorageEntries(t *testing.T) {
	entries := index(3).terms(3, 4, 5, 5, 6, 7, 7, 7, 7, 8)
	l := storage.NewLog(tempLog(t))
	l.Append(entries)

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
			AssertStructsEqual(t, got, tt.wantEntries)
		})
	}
}

func TestStorageTerm(t *testing.T) {
	t.Run("for empty storage", func(t *testing.T) {
		l := storage.NewLog(tempLog(t))

		fi, _ := l.FirstIndex()
		_, err := l.Term(fi)
		AssertErrorIs(t, err, raft.ErrUnavailable)

		li, _ := l.LastIndex()
		_, err = l.Term(li)
		AssertNoError(t, err)

		_, err = l.Term(li + 1)
		AssertErrorIs(t, err, raft.ErrUnavailable)
	})

	t.Run("for storage with entries", func(t *testing.T) {
		ents := index(3).terms(3, 4, 4, 5)
		tests := []struct {
			name string
			i    uint64

			werr  error
			wterm uint64
		}{
			{"index lower than FirstIndex() returns ErrCompacted",
				2, raft.ErrCompacted, 0},
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
				l := storage.NewLog(tempLog(t))
				l.Append(ents)

				term, err := l.Term(tt.i)
				AssertErrorIs(t, err, tt.werr).Require()
				AssertEqual(t, term, tt.wterm).Require()
			})
		}
	})
}

func TestStorageEntries2(t *testing.T) {
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
			s := storage.NewLog(tempLog(t))
			s.Append(ents)

			entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
			AssertErrorIs(t, err, tt.werr)
			require.Equal(t, tt.wentries, entries)
		})
	}
}

func TestStorageLastIndex(t *testing.T) {
	l := storage.NewLog(tempLog(t))

	var want uint64
	got, err := l.LastIndex()
	AssertNoError(t, err).Require()
	AssertEqual(t, got, want)

	entries := index(3).terms(3, 4, want)
	want = entries[len(entries)-1].Index
	l.Append(entries)

	got, err = l.LastIndex()
	AssertNoError(t, err).Require()
	AssertEqual(t, got, want)
}

func TestStorageFirstIndex(t *testing.T) {
	l := storage.NewLog(tempLog(t))
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
		l.Append(entries)

		got, err := l.FirstIndex()
		AssertNoError(t, err).Require()
		AssertEqual(t, got, want)
	})
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
