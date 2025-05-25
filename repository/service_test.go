package repository_test

import (
	"testing"

	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/store"
	"github.com/robinkb/cascade-registry/store/inmemory"
	"github.com/stretchr/testify/suite"
)

type Constructor func() (repository.RepositoryService, store.Metadata, store.Blobs)

type Suite struct {
	suite.Suite

	Constructor Constructor

	repository repository.RepositoryService
	metadata   store.Metadata
	blobs      store.Blobs
}

func (s *Suite) SetupSuite() {
	s.repository, s.metadata, s.blobs = s.Constructor()
}

func TestWithInMemory(t *testing.T) {
	suite.Run(t, &Suite{
		Constructor: func() (repository.RepositoryService, store.Metadata, store.Blobs) {
			metadata := inmemory.NewMetadataStore()
			blobs := inmemory.NewBlobStore()
			repository := repository.NewRepositoryService(metadata, blobs)

			return repository, metadata, blobs
		},
	})
}
