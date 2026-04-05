package registry

import (
	"sync"
	"testing"

	"github.com/robinkb/cascade/registry/repository"
	"github.com/robinkb/cascade/registry/store"
	"github.com/robinkb/cascade/registry/store/driver/boltdb"
	"github.com/robinkb/cascade/registry/store/driver/inmemory"
	. "github.com/robinkb/cascade/testing"
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
				metadata, err := boltdb.NewMetadataStore(t.TempDir())
				AssertNoError(t, err).Require()
				blobs := inmemory.NewBlobStore()
				return metadata, blobs
			},
		},
	}

	for _, tt := range tests {
		service := New(tt.constructor())

		t.Run(tt.description, func(t *testing.T) {
			t.Run("Create a repository, do something with it, and delete it", func(t *testing.T) {
				name := RandomName()
				repo, err := service.CreateRepository(name)
				AssertNoError(t, err)

				id, _, content := RandomManifest()
				_, err = repo.PutManifest(id.String(), content)
				RequireNoError(t, err)
				_, _, err = repo.GetManifest(id.String())
				AssertNoError(t, err)

				err = service.DeleteRepository(name)
				AssertNoError(t, err)
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

			t.Run("Attempting to get the same repository concurrently doesn't fail", func(t *testing.T) {
				// This can occur when repositories are created ad-hoc during an image push,
				// where multiple layers are pushed concurrently.
				name := RandomName()
				routines := 10

				var wg sync.WaitGroup
				for range routines {
					wg.Go(func() {
						_, err := service.GetRepository(name)
						AssertNoError(t, err)
					})
				}
				wg.Wait()
			})
		})
	}
}
