package logdeck

import (
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
	"golang.org/x/exp/mmap"
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
	want := randomRecordsN(10, 16, 32)

	l := newLog(tempLog(t))

	for i := range want {
		err := l.Append(want[i])
		AssertNoError(t, err).Require()
	}

	l.Rewind()

	i := 0
	for got := range l.All() {
		AssertDeepEqual(t, got, want[i])
		i++
	}
}

// The decoder normally keeps reading until EOF.
// But with a pre-allocated file, a lot of the file might be empty.
// This is easiest to test using the Log, but the actual fix
// is in the decoder. It treats reading an empty header as EOF.
func TestLogReadAllPreallocated(t *testing.T) {
	name := filepath.Join(t.TempDir(), RandomString(5)+".log")
	w, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0644)
	AssertNoError(t, err).Require()
	err = syscall.Fallocate(int(w.Fd()), 0, 0, 1<<10)
	AssertNoError(t, err).Require()
	r, err := mmap.Open(name)
	AssertNoError(t, err).Require()

	log := newLog(r, w)

	want := randomRecordsN(10, 16, 32)
	for i := range want {
		err := log.Append(want[i])
		AssertNoError(t, err).Require()
	}

	i := 0
	for got := range log.All() {
		AssertEqual(t, got, want[i])
		i++
	}
}
