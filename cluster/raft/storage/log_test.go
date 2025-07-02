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

func tempLog(t *testing.T) (io.ReadSeeker, io.Writer) {
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

// These tests come from etcd-io/raft.
func TestStorageTerm(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	tests := []struct {
		i uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, raft.ErrCompacted, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, raft.ErrUnavailable, 0, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			l := storage.NewLog(tempLog(t))
			l.Append(ents)

			if tt.wpanic {
				require.Panics(t, func() {
					_, _ = l.Term(tt.i)
				})
			}
			term, err := l.Term(tt.i)
			require.Equal(t, tt.werr, err)
			require.Equal(t, tt.wterm, term)
		})
	}
}

func TestStorageEntries(t *testing.T) {
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
	ents := index(3).terms(3, 4, 5)
	l := storage.NewLog(tempLog(t))
	l.Append(ents)

	last, err := l.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), last)

	require.NoError(t, l.Append(index(6).terms(5)))
	last, err = l.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), last)
}

func TestStorageFirstIndex(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	l := storage.NewLog(tempLog(t))
	l.Append(ents)

	first, err := l.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), first)

	// require.NoError(t, s.Compact(4))
	// first, err = s.FirstIndex()
	// require.NoError(t, err)
	// require.Equal(t, uint64(5), first)
}

func TestStorageEmpty(t *testing.T) {
	l := storage.NewLog(tempLog(t))
	fi, err := l.FirstIndex()
	AssertNoError(t, err)
	AssertEqual(t, fi, 1)

	li, err := l.LastIndex()
	AssertNoError(t, err)
	AssertEqual(t, li, 0)
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
