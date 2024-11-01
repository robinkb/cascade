package cascade

import (
	"errors"

	"github.com/opencontainers/go-digest"
)

func (s *registryService) ListTags(repository string) ([]string, error) {
	return s.metadata.ListTags(repository)
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

	// TODO: Add test case for passing invalid digest.
	digest, _ := digest.Parse(id)

	return s.metadata.PutTag(repository, tag, digest)
}

func (s *registryService) DeleteTag(repository, tag string) error {
	return s.metadata.DeleteTag(repository, tag)
}
