package repository

import (
	"testing"

	"github.com/robinkb/cascade/registry/store"
	"github.com/robinkb/cascade/registry/store/driver/boltdb"
	"github.com/robinkb/cascade/registry/store/driver/fs"
	"github.com/robinkb/cascade/registry/store/driver/inmemory"
	. "github.com/robinkb/cascade/testing"
	"github.com/stretchr/testify/suite"
)

func NewTestRepository(t *testing.T) (store.Blobs, store.Repository, Service) {
	blobs := inmemory.NewBlobStore()
	meta := inmemory.NewMetadataStore()
	repo, err := meta.CreateRepository(RandomName())
	AssertNoError(t, err).Require()
	return blobs, repo, NewRepositoryService(blobs, repo)
}

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
}

func (s *Suite) RandomRepository() Service {
	repo, err := s.metadata.CreateRepository(RandomName())
	RequireNoError(s.T(), err)
	return NewRepositoryService(s.blobs, repo)
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
			metadata, err := boltdb.NewMetadataStore(t.TempDir())
			AssertNoError(t, err).Require()
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
