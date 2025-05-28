package cascade_test

import (
	"sync"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/repository"
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
			t.Run("Create a repository, do something with it, and delete it", func(t *testing.T) {
				name := RandomName()
				err := service.CreateRepository(name)
				AssertNoError(t, err)

				repo, err := service.GetRepository(name)
				AssertNoError(t, err)

				id, _, content := RandomManifest()
				_, err = repo.PutManifest(name, id.String(), content)
				RequireNoError(t, err)
				_, _, err = repo.GetManifest(name, id.String())
				AssertNoError(t, err)

				err = service.DeleteRepository(name)
				AssertNoError(t, err)

				_, _, err = repo.GetManifest(name, id.String())
				AssertErrorIs(t, err, repository.ErrNameUnknown)
			})

			t.Run("An unknown repository is automatically created", func(t *testing.T) {
				name := RandomName()
				_, err := service.GetRepository(name)
				AssertNoError(t, err)
			})

			t.Run("Retrieving an unknown repository fails", func(t *testing.T) {
				// Maybe at some point we'll enforce creating repositories explicitly.
				// Or provide the option to enforce it.
				t.SkipNow()
				name := RandomName()
				_, err := service.GetRepository(name)
				AssertErrorIs(t, err, repository.ErrNameUnknown)
			})

			t.Run("Attempting to create the same repository concurrently doesn't fail", func(t *testing.T) {
				// This can occur when repositories are created ad-hoc during an image push,
				// where multiple layers are pushed concurrently.
				name := RandomName()
				routines := 3

				var wg sync.WaitGroup
				wg.Add(routines)
				for range routines {
					go func() {
						err := service.CreateRepository(name)
						AssertNoError(t, err)
						wg.Done()
					}()
				}
				wg.Wait()
			})
		})
	}
}
