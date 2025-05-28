package cascade_test

import (
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/store"
	"github.com/robinkb/cascade-registry/store/boltdb"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

type StoreConstructor func() (store.Metadata, store.Blobs)

func TestRepository(t *testing.T) {
	tests := []struct {
		description string
		constructor StoreConstructor
	}{
		{
			"With InMemory metadata store",
			func() (store.Metadata, store.Blobs) {
				return inmemory.NewMetadataStore(), inmemory.NewBlobStore()
			},
		},
		{
			"With BoltDB metadata store",
			func() (store.Metadata, store.Blobs) {
				metadata := boltdb.NewMetadataStore(t.TempDir())
				blobs := inmemory.NewBlobStore()
				return metadata, blobs
			},
		},
	}

	for _, tt := range tests {
		service := cascade.NewRegistryService(tt.constructor())

		t.Run(tt.description, func(t *testing.T) {
			t.Run("Create a repository, do something with it, and remove it", func(t *testing.T) {
				name := RandomName()
				err := service.CreateRepository(name)
				AssertNoError(t, err)

				repo, err := service.GetRepository(name)
				AssertNoError(t, err)

				id, _, content := RandomManifest()
				_, err = repo.PutManifest(name, id.String(), content)
				RequireNoError(t, err)
			})

			t.Run("Retrieve a repository", func(t *testing.T) {
				name := RandomName()
				_, err := service.GetRepository(name)
				AssertNoError(t, err)
			})
		})
	}
}
