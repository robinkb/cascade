package cascade

import (
	"errors"

	"github.com/robinkb/cascade-registry/paths"
)

func (s *registryService) GetTag(repository, tag string) (string, error) {
	if !ValidateTag(tag) {
		return "", ErrTagInvalid
	}

	tagLink := paths.MetaStore.TagLink(repository, tag)
	digest, err := s.store.Get(tagLink)

	return string(digest), err
}

func (s *registryService) PutTag(repository, tag, digest string) error {
	if !ValidateTag(tag) {
		return ErrTagInvalid
	}

	tagLink := paths.MetaStore.TagLink(repository, tag)
	return s.store.Set(tagLink, []byte(digest))
}

func (s *registryService) DeleteTag(repository, tag string) error {
	return errors.New("not implemented")
}
