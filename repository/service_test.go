package repository_test

import (
	"testing"

	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/store"
	"github.com/robinkb/cascade-registry/store/boltdb"
	"github.com/robinkb/cascade-registry/store/fs"
	"github.com/robinkb/cascade-registry/store/inmemory"
	"github.com/stretchr/testify/suite"
)

type StoreConstructor func() (store.Metadata, store.Blobs)

type Suite struct {
	suite.Suite

	StoreConstructor StoreConstructor
	Tests            Tests

	repository repository.RepositoryService
	metadata   store.Metadata
	blobs      store.Blobs
}

// Tests allows selectively disabling tests in the suite. Useful when developing new store backends.
// The Tests struct roughly lists the tests in order of easiest to hardest, so it is recommend
// to make the tests pass in that order.
type Tests struct {
	BlobsDisabled      bool
	ManifestsDisbabled bool
	TagsDisabled       bool
	UploadsDisabled    bool
	ReferrersDisabled  bool
}

func (s *Suite) SetupSuite() {
	s.metadata, s.blobs = s.StoreConstructor()
	s.repository = repository.NewRepositoryService(s.metadata, s.blobs)
}

func TestWithInMemoryStore(t *testing.T) {
	suite.Run(t, &Suite{
		StoreConstructor: func() (store.Metadata, store.Blobs) {
			metadata := inmemory.NewMetadataStore()
			blobs := inmemory.NewBlobStore()

			return metadata, blobs
		},
	})
}

func TestWithBoltDBStore(t *testing.T) {
	suite.Run(t, &Suite{
		StoreConstructor: func() (store.Metadata, store.Blobs) {
			metadata := boltdb.NewMetadataStore(t.TempDir())
			blobs := inmemory.NewBlobStore()

			return metadata, blobs
		},
		Tests: Tests{
			BlobsDisabled:      false,
			ManifestsDisbabled: true,
			TagsDisabled:       true,
			UploadsDisabled:    true,
			ReferrersDisabled:  true,
		},
	})
}

func TestWithFilesystemStore(t *testing.T) {
	suite.Run(t, &Suite{
		StoreConstructor: func() (store.Metadata, store.Blobs) {
			metadata := inmemory.NewMetadataStore()
			blobs := fs.NewBlobStore(t.TempDir())

			return metadata, blobs
		},
	})
}
