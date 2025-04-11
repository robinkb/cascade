package cascade

import (
	"errors"

	"github.com/opencontainers/go-digest"
)

func (s *registryService) ListTags(repository string, count int, last string) ([]string, error) {
	return s.metadata.ListTags(repository, count, last)
}

func (s *registryService) GetTag(repository, tag string) (string, error) {
	if !ValidateTag(tag) {
		return "", ErrTagInvalid
	}

	digest, err := s.metadata.GetTag(repository, tag)
	if errors.Is(err, ErrFileNotFound) {
		err = ErrManifestUnknown
	}

	return digest.String(), err
}

func (s *registryService) PutTag(repository, tag, id string) error {
	if !ValidateTag(tag) {
		return ErrTagInvalid
	}

	digest, err := digest.Parse(id)
	if err != nil {
		return err
	}

	return s.metadata.PutTag(repository, tag, digest)
}

func (s *registryService) DeleteTag(repository, tag string) error {
	return s.metadata.DeleteTag(repository, tag)
}
