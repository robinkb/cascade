package repository

import (
	"errors"

	"github.com/opencontainers/go-digest"

	"github.com/robinkb/cascade/registry/store"
)

func (s *repositoryService) ListTags(count int, last string) ([]string, error) {
	return s.repo.ListTags(count, last)
}

func (s *repositoryService) GetTag(tag string) (string, error) {
	if !ValidateTag(tag) {
		return "", ErrTagInvalid
	}

	digest, err := s.repo.GetTag(tag)
	if errors.Is(err, store.ErrTagNotFound) {
		err = ErrManifestUnknown
	}

	return digest.String(), err
}

func (s *repositoryService) PutTag(tag, id string) error {
	if !ValidateTag(tag) {
		return ErrTagInvalid
	}

	digest, err := digest.Parse(id)
	if err != nil {
		return err
	}

	deleted, err := s.repo.PutTag(tag, digest)
	if errors.Is(err, store.ErrRepositoryNotFound) {
		err = ErrNameUnknown
	}

	s.collect(deleted)

	return err
}

func (s *repositoryService) DeleteTag(tag string) error {
	deleted, err := s.repo.DeleteTag(tag)
	if err != nil {
		return err
	}

	s.collect(deleted)

	return nil
}
