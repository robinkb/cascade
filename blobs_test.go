package cascade_test

import (
	"io"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestStatBlob(t *testing.T) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)

	name := RandomName()
	name, digest, content := randomBlob(32 * 1024)
	path := digest.Encoded()

	blobs.Put(path, content)
	metadata.PutBlob(name, digest, path)

	t.Run("Known blob returns no error", func(t *testing.T) {
		_, err := service.StatBlob(name, digest.String())
		AssertNoError(t, err)
	})

	t.Run("Unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.StatBlob("a", "b")
		assertErrorIs(t, err, cascade.ErrBlobUnknown)
	})

	t.Run("Known blob in unknown repository returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.StatBlob("fake/repository", digest.String())
		assertErrorIs(t, err, cascade.ErrBlobUnknown)
	})
}

func TestGetBlob(t *testing.T) {
	service, metadata, blobs := newTestRegistry()

	name, digest, content := randomBlob(32)
	path := digest.Encoded()

	blobs.Put(path, content)
	metadata.PutBlob(name, digest, path)

	t.Run("Known blob returns content and no error", func(t *testing.T) {
		r, err := service.GetBlob(name, digest.String())
		assertNoError(t, err)
		data, err := io.ReadAll(r)
		assertNoError(t, err)
		assertContent(t, data, content)
	})

	t.Run("Unknown blob returns no content and ErrBlobUnknown", func(t *testing.T) {
		_, err := service.GetBlob("fake/repository", "blabla")
		assertErrorIs(t, err, cascade.ErrBlobUnknown)
	})

	t.Run("Known blob on unknown repository still returns no content and ErrBlobUnknown", func(t *testing.T) {
		_, err := service.GetBlob("fake/repository", digest.String())
		assertErrorIs(t, err, cascade.ErrBlobUnknown)
	})
}

func TestDeleteBlob(t *testing.T) {
	service, metadata, blobs := newTestRegistry()

	name, digest, content := randomBlob(32)
	path := digest.String()

	t.Run("A deleted blob is not retrievable", func(t *testing.T) {
		metadata.PutBlob(name, digest, path)
		blobs.Put(path, content)

		_, err := service.GetBlob(name, digest.String())
		assertNoError(t, err)

		err = service.DeleteBlob(name, digest.String())
		assertNoError(t, err)

		_, err = service.GetBlob(name, digest.String())
		assertErrorIs(t, err, cascade.ErrBlobUnknown)
	})
}
