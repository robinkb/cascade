package main

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestServiceStatBlob(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.StatBlob("a", "b")
		assertErrorIs(t, err, ErrBlobUnknown)
	})
}

func TestServiceGetBlob(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.GetBlob("a")
		assertErrorIs(t, err, ErrBlobUnknown)
	})
}

func TestServiceWriteBlob(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("blobs are stored in a Merkle tree by their digest", func(t *testing.T) {
		content := []byte("my big beautiful blob")
		algorithm := "sha256"
		sum := "af2f9984c0dcaa963e20a4eae0e57c186898a8856148dd285cc68d7fe21779b8"
		digest := fmt.Sprintf("%s:%s", algorithm, sum)

		// Is this test useful? This line is literally the same as in the tested code.
		path := fmt.Sprintf("blobs/%s/%s/%s", algorithm, sum[0:2], sum)

		err := service.WriteBlob(digest, content)
		assertNoError(t, err)

		got, err := store.Get(path)
		assertNoError(t, err)

		if !bytes.Equal(got, content) {
			t.Errorf("unexpected byte content")
		}
	})
}

func TestServiceStatManifest(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("returns FileInfo on known manifest", func(t *testing.T) {
		content := randomContents(32)
		store.Put("manifests/library/fedora/1.0.0", content)
		info, err := service.StatManifest("library/fedora", "1.0.0")

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("got size of %d, expected %d", got, want)
		}
		assertNoError(t, err)
	})

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.StatManifest("do/not", "exist")
		assertErrorIs(t, err, ErrManifestUnknown)
	})
}

func TestServiceGetManifest(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.GetManifest("i/do/not/exist", "for-real")
		assertErrorIs(t, err, ErrManifestUnknown)
	})
}

func assertErrorIs(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Errorf("unexpected error: got %q, want %q", got, want)
	}
}
