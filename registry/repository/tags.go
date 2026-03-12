package repository

import (
	"errors"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

func (s *repositoryService) ListTags(repository string, count int, last string) ([]string, error) {
	return s.metadata.ListTags(repository, count, last)
}

func (s *repositoryService) GetTag(repository, tag string) (string, error) {
	if !ValidateTag(tag) {
		return "", ErrTagInvalid
	}

	digest, err := s.metadata.GetTag(repository, tag)
	if errors.Is(err, store.ErrNotFound) {
		err = ErrManifestUnknown
	}

	return digest.String(), err
}

func (s *repositoryService) PutTag(repository, tag, id string) error {
	if !ValidateTag(tag) {
		return ErrTagInvalid
	}

	digest, err := digest.Parse(id)
	if err != nil {
		return err
	}

	err = s.metadata.PutTag(repository, tag, digest)
	if errors.Is(err, store.ErrRepositoryNotFound) {
		err = ErrNameUnknown
	}
	return err
}

func (s *repositoryService) DeleteTag(repository, tag string) error {
	return s.metadata.DeleteTag(repository, tag)
}
