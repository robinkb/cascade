package repository_test

import (
	"testing"

	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/store"
	"github.com/robinkb/cascade-registry/store/fs"
	"github.com/robinkb/cascade-registry/store/inmemory"
	"github.com/stretchr/testify/suite"
)

type StoreConstructor func() (store.Metadata, store.Blobs)

type Suite struct {
	suite.Suite

	StoreConstructor StoreConstructor

	repository repository.RepositoryService
	metadata   store.Metadata
	blobs      store.Blobs
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

func TestWithFilesystemStore(t *testing.T) {
	suite.Run(t, &Suite{
		StoreConstructor: func() (store.Metadata, store.Blobs) {
			metadata := inmemory.NewMetadataStore()
			blobs := fs.NewBlobStore(t.TempDir())

			return metadata, blobs
		},
	})
}
