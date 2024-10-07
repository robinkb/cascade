package main

import (
	"bytes"
	"testing"
)

func TestStat(t *testing.T) {
	store := NewInMemoryStore()

	t.Run("Stat returns no errors and a FileInfo on existing file", func(t *testing.T) {
		content := randomContents(32)
		err := store.Put("a/b/c", bytes.NewBuffer(content))
		assertNoError(t, err)

		info, err := store.Stat("a/b/c")
		assertNoError(t, err)

		got := info.Size
		want := int64(len(content))
		if got != want {
			t.Errorf("unexpected file size returned; got %d, want %d", got, want)
		}
	})
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("got error where none was expected: %v", err)
	}
}
