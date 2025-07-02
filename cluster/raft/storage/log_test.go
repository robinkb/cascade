package storage

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
	"go.etcd.io/raft/v3/raftpb"
)

func TestNewLog(t *testing.T) {
	r, w := tempLog(t)
	entries := index(3).terms(3, 3, 4, 4, 5, 6, 7, 8)

	l := NewLog(r, w)
	l.Append(entries)
}

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
