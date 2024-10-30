package cascade

import (
	"errors"

	"github.com/robinkb/cascade-registry/paths"
)

func (s *registryService) ListTags(repository string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (s *registryService) GetTag(repository, tag string) (string, error) {
	if !ValidateTag(tag) {
		return "", ErrTagInvalid
	}

	tagLink := paths.MetaStore.TagLink(repository, tag)
	digest, err := s.store.Get(tagLink)
	if errors.Is(err, ErrFileNotFound) {
		err = ErrManifestUnknown
	}

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
	tagLink := paths.MetaStore.TagLink(repository, tag)
	return s.store.Delete(tagLink)
}
