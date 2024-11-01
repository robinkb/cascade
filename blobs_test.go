package cascade

import (
	"io"
	"testing"

	"github.com/robinkb/cascade-registry/paths"
)

func TestStatBlob(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	name, digest, content := randomBlob(32 * 1024)

	service.b.Put(paths.BlobStore.BlobData(digest), content)
	store.Set(paths.MetaStore.BlobLink(name, digest), nil)

	t.Run("Known blob returns no error", func(t *testing.T) {
		_, err := service.StatBlob(name, digest.String())
		assertNoError(t, err)
	})

	t.Run("Unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.StatBlob("a", "b")
		assertErrorIs(t, err, ErrBlobUnknown)
	})

	t.Run("Known blob in unknown repository returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.StatBlob("fake/repository", digest.String())
		assertErrorIs(t, err, ErrBlobUnknown)
	})
}

func TestGetBlob(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	name, digest, content := randomBlob(32)

	service.b.Put(paths.BlobStore.BlobData(digest), content)
	store.Set(paths.MetaStore.BlobLink(name, digest), nil)

	t.Run("Known blob returns content and no error", func(t *testing.T) {
		r, err := service.GetBlob(name, digest.String())
		assertNoError(t, err)
		data, err := io.ReadAll(r)
		assertNoError(t, err)
		assertContent(t, data, content)
	})

	t.Run("Unknown blob returns no content and ErrBlobUnknown", func(t *testing.T) {
		_, err := service.GetBlob("fake/repository", "blabla")
		assertErrorIs(t, err, ErrBlobUnknown)
	})

	t.Run("Known blob on unknown repository still returns no content and ErrBlobUnknown", func(t *testing.T) {
		_, err := service.GetBlob("fake/repository", digest.String())
		assertErrorIs(t, err, ErrBlobUnknown)
	})
}
