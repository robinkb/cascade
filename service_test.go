package main

import (
	"errors"
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
		_, err := service.GetBlob("a", "b")
		assertErrorIs(t, err, ErrBlobUnknown)
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
