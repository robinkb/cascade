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
	digest, content := RandomBlob(32 * 1024)

	err := blobs.PutBlob(digest, content)
	RequireNoError(t, err)
	err = metadata.PutBlob(name, digest)
	RequireNoError(t, err)

	t.Run("Known blob returns no error", func(t *testing.T) {
		_, err := service.StatBlob(name, digest.String())
		AssertNoError(t, err)
	})

	t.Run("Unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.StatBlob("a", "b")
		AssertErrorIs(t, err, cascade.ErrBlobUnknown)
	})

	t.Run("Known blob in unknown repository returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.StatBlob("fake/repository", digest.String())
		AssertErrorIs(t, err, cascade.ErrBlobUnknown)
	})
}

func TestGetBlob(t *testing.T) {
	service, metadata, blobs := newTestRegistry()

	name := RandomName()
	digest, content := RandomBlob(32)

	err := blobs.PutBlob(digest, content)
	RequireNoError(t, err)
	err = metadata.PutBlob(name, digest)
	RequireNoError(t, err)

	t.Run("Known blob returns content and no error", func(t *testing.T) {
		r, err := service.GetBlob(name, digest.String())
		AssertNoError(t, err)
		data, err := io.ReadAll(r)
		AssertNoError(t, err)
		AssertSlicesEqual(t, data, content)
	})

	t.Run("Unknown blob returns no content and ErrBlobUnknown", func(t *testing.T) {
		_, err := service.GetBlob("fake/repository", "blabla")
		AssertErrorIs(t, err, cascade.ErrBlobUnknown)
	})

	t.Run("Known blob on unknown repository still returns no content and ErrBlobUnknown", func(t *testing.T) {
		_, err := service.GetBlob("fake/repository", digest.String())
		AssertErrorIs(t, err, cascade.ErrBlobUnknown)
	})
}

func TestDeleteBlob(t *testing.T) {
	service, metadata, blobs := newTestRegistry()

	name := RandomName()
	digest, content := RandomBlob(32)

	t.Run("A deleted blob is not retrievable", func(t *testing.T) {
		err := blobs.PutBlob(digest, content)
		RequireNoError(t, err)
		err = metadata.PutBlob(name, digest)
		RequireNoError(t, err)

		_, err = service.GetBlob(name, digest.String())
		RequireNoError(t, err)

		err = service.DeleteBlob(name, digest.String())
		RequireNoError(t, err)

		_, err = service.GetBlob(name, digest.String())
		AssertErrorIs(t, err, cascade.ErrBlobUnknown)
	})
}
