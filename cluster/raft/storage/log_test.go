package storage

import (
	"io"
	"os"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestNewLog(t *testing.T) {
	r, w := tempLog(t)
	enc := NewEncoder(w)

	for range 10 {
		err := enc.Encode(randomRecord(128))
		AssertNoError(t, err).Require()
	}

	NewLog(r, w)
}

func tempLog(t *testing.T) (io.ReadSeeker, io.Writer) {
	filename := t.TempDir() + "log"
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
