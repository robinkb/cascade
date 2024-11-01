package cascade

import (
	"testing"

	"github.com/robinkb/cascade-registry/paths"
)

func TestStatBlob(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	name, digest, content := randomBlob(32 * 1024)

	store.Set(paths.MetaStore.BlobLink(name, digest), nil)
	w, _ := service.b.Writer(paths.BlobStore.BlobData(digest))
	w.Write(content)

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

	w, _ := service.b.Writer(paths.BlobStore.BlobData(digest))
	w.Write(content)
	store.Set(paths.MetaStore.BlobLink(name, digest), nil)

	t.Run("Known blob returns content and no error", func(t *testing.T) {
		data, err := service.GetBlob(name, digest.String())
		assertContent(t, data, content)
		assertNoError(t, err)
	})

	t.Run("Unknown blob returns no content and ErrBlobUnknown", func(t *testing.T) {
		data, err := service.GetBlob("fake/repository", "blabla")
		assertContent(t, data, nil)
		assertErrorIs(t, err, ErrBlobUnknown)
	})

	t.Run("Known blob on unknown repository still returns no content and ErrBlobUnknown", func(t *testing.T) {
		data, err := service.GetBlob("fake/repository", digest.String())
		assertContent(t, data, nil)
		assertErrorIs(t, err, ErrBlobUnknown)
	})
}
