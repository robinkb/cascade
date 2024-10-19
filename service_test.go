package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/robinkb/cascade-registry/paths"
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

func TestServiceUpload(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("stat upload returns correct FileInfo", func(t *testing.T) {
		repository := "a/v/c"
		sessionID := "123"
		content := randomContents(32)

		store.Set(paths.MetaStore.UploadLink(repository, sessionID), nil)
		store.Set(paths.BlobStore.UploadData(sessionID), content)

		info, err := service.StatUpload(repository, sessionID)
		assertNoError(t, err)

		got := info.Size
		want := len(content)

		if info.Size != int64(len(content)) {
			t.Errorf("got unexpected upload size %d, want %d", got, want)
		}
	})

	t.Run("stat upload on unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		_, err := service.StatUpload("unknown/repo", "i-dont-exist")
		assertErrorIs(t, err, ErrBlobUploadUnknown)
	})

	t.Run("written upload is retrievable", func(t *testing.T) {
		repository := "a/b/c"
		content := randomContents(32)

		session := service.InitUpload(repository)

		err := service.AppendUpload(repository, session.ID, content)
		assertNoError(t, err)

		got, err := store.Get(paths.BlobStore.UploadData(session.ID))
		assertNoError(t, err)

		if !reflect.DeepEqual(got, content) {
			t.Errorf("got unexpected byte content; not equal to written content")
		}
	})

	t.Run("writing multiple times to same upload appends", func(t *testing.T) {
		repository := "1/2/3"
		content := randomContents(32)

		session := service.InitUpload(repository)

		err := service.AppendUpload(repository, session.ID, content[:16])
		assertNoError(t, err)

		err = service.AppendUpload(repository, session.ID, content[16:])
		assertNoError(t, err)

		info, err := service.StatUpload(repository, session.ID)
		assertNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("unexpected upload size; got %d, want %d", got, want)
		}
	})

	t.Run("writing to unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		err := service.AppendUpload("1/2/3", "i-dont-exist", []byte{})
		assertErrorIs(t, err, ErrBlobUploadUnknown)
	})
}

func TestServiceStatManifest(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("returns FileInfo on known manifest", func(t *testing.T) {
		repository := "asflkn/waekln"
		content := randomContents(32)
		digest := fmt.Sprintf("sha256:%x", sha256.Sum256(content))

		err := service.PutManifest(repository, digest, content)
		assertNoError(t, err)

		info, err := service.StatManifest(repository, digest)
		assertNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("got size of %d, expected %d", got, want)
		}
		assertNoError(t, err)
	})

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.StatManifest("do/not", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		assertErrorIs(t, err, ErrManifestUnknown)
	})
}

func TestServiceGetManifest(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.GetManifest("i/do/not/exist", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		assertErrorIs(t, err, ErrManifestUnknown)
	})
}

func assertErrorIs(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Errorf("unexpected error: got %q, want %q", got, want)
	}
}
