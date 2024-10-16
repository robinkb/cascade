package main

import (
	"crypto/sha256"
	"encoding"
	"errors"
	"fmt"
	"reflect"
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

func TestServiceUpload(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("stat upload returns correct FileInfo", func(t *testing.T) {
		sessionID := "123"
		content := randomContents(32)
		store.Set(fmt.Sprintf("uploads/%s/data", sessionID), content)

		info, err := service.StatUpload(sessionID)
		assertNoError(t, err)

		got := info.Size
		want := len(content)

		if info.Size != int64(len(content)) {
			t.Errorf("got unexpected upload size %d, want %d", got, want)
		}
	})

	t.Run("stat upload on unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		_, err := service.StatUpload("i-dont-exist")
		assertErrorIs(t, err, ErrBlobUploadUnknown)
	})

	t.Run("written upload is retrievable", func(t *testing.T) {
		sessionID := "123"
		path := fmt.Sprintf("uploads/%s/data", sessionID)
		content := randomContents(32)

		hashPath := fmt.Sprintf("uploads/%s/hashstate/sha256", sessionID)
		hashState, _ := sha256.New().(encoding.BinaryMarshaler).MarshalBinary()

		store.Set(path, []byte{})
		store.Set(hashPath, hashState)

		err := service.WriteUpload(sessionID, content)
		assertNoError(t, err)

		got, err := store.Get(path)
		assertNoError(t, err)

		if !reflect.DeepEqual(got, content) {
			t.Errorf("got unexpected byte content; not equal to written content")
		}
	})

	t.Run("writing multiple times to same upload appends", func(t *testing.T) {
		sessionID := "123"
		path := fmt.Sprintf("uploads/%s/data", sessionID)
		content := randomContents(32)

		hashPath := fmt.Sprintf("uploads/%s/hashstate/sha256", sessionID)
		hashState, _ := sha256.New().(encoding.BinaryMarshaler).MarshalBinary()

		store.Set(path, []byte{})
		store.Set(hashPath, hashState)

		err := service.WriteUpload(sessionID, content[:16])
		assertNoError(t, err)

		err = service.WriteUpload(sessionID, content[16:])
		assertNoError(t, err)

		info, err := service.StatUpload(sessionID)
		assertNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("unexpected upload size; got %d, want %d", got, want)
		}
	})

	t.Run("writing to unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		err := service.WriteUpload("i-dont-exist", []byte{})
		assertErrorIs(t, err, ErrBlobUploadUnknown)
	})
}

func TestServiceStatManifest(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("returns FileInfo on known manifest", func(t *testing.T) {
		content := randomContents(32)
		store.Set("manifests/library/fedora/1.0.0", content)
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
