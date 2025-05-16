package cascade_test

import (
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestGetRepository(t *testing.T) {
	service := cascade.NewRegistryService2(inmemory.NewMetadataStore(), inmemory.NewBlobStore())
	name := RandomName()
	t.Run("Retrieve a repository", func(t *testing.T) {
		_, err := service.GetRepository(name)
		AssertNoError(t, err)
	})
}

func newTestRepository() (cascade.RepositoryService, cascade.MetadataStore, cascade.BlobStore) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)

	return service, metadata, blobs
}
