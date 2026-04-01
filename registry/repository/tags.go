package repository

import (
	"errors"
	"log"
	"sync"

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

	err = s.repo.PutTag(tag, digest)
	if errors.Is(err, store.ErrRepositoryNotFound) {
		err = ErrNameUnknown
	}
	return err
}

func (s *repositoryService) DeleteTag(tag string) error {
	deleted, err := s.repo.DeleteTag(tag)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, id := range deleted {
		wg.Go(func() {
			if err := s.blobs.DeleteBlob(id); err != nil {
				log.Printf("failed to garbage collect blob with digest %s: %s", id, err)
			}
		})
	}
	wg.Wait()

	return nil
}
