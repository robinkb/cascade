package repository

import (
	"testing"

	"github.com/robinkb/cascade-registry/registry/store"
	"github.com/robinkb/cascade-registry/registry/store/boltdb"
	"github.com/robinkb/cascade-registry/registry/store/fs"
	"github.com/robinkb/cascade-registry/registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/stretchr/testify/suite"
)

// StoreConstructor is a function that returns a Metadata and Blobs store
// for use in the testing suite. It is only called once during setup.
type StoreConstructor func() (store.Metadata, store.Blobs)

// Suite runs the testing suite for the Repository service.
// It is used to easily validate the RepositoryService with different
// store backends.
type Suite struct {
	suite.Suite

	StoreConstructor StoreConstructor
	Tests            Tests

	repository Service
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
	s.repository = NewRepositoryService(s.metadata, s.blobs)
}

func (s *Suite) RandomRepository() string {
	name := RandomName()
	err := s.metadata.CreateRepository(name)
	RequireNoError(s.T(), err)
	return name
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
