package storage_test

import (
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	. "github.com/robinkb/cascade-registry/testing"
)

func tempLog(t *testing.T) (io.ReaderAt, io.Writer) {
	filename := filepath.Join(t.TempDir(), "log.bin")
	w, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	AssertNoError(t, err).Require()

	r, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	AssertNoError(t, err).Require()

	t.Cleanup(func() {
		err := r.Close()
		if err != nil {
			t.Log("error closing read handler for temporary log:", err)
		}
		err = w.Close()
		if err != nil {
			t.Log("error closing write handler for temporary log:", err)
		}
	})

	return r, w
}

func TestLogReadAll(t *testing.T) {
	l := storage.NewLog(tempLog(t))

	l.All()

	want := make([]*storage.Record, 10)
	for i := range want {
		want[i] = randomRecord(rand.Int64N(16) + 16)
	}

	for i := range want {
		err := l.Append(want[i])
		AssertNoError(t, err).Require()
	}

	l.Rewind()

	i := 0
	for got := range l.All() {
		AssertStructsEqual(t, got, want[i])
		i++
	}
}
