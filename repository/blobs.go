package repository

import (
	"errors"
	"io"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
)

func (s *repositoryService) StatBlob(repository, id string) (*store.BlobInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	_, err = s.metadata.GetBlob(repository, digest)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			err = ErrBlobUnknown
		}
		return nil, err
	}

	return s.blobs.StatBlob(digest)
}

func (s *repositoryService) GetBlob(repository, id string) (io.Reader, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	_, err = s.metadata.GetBlob(repository, digest)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			err = ErrBlobUnknown
		}
		return nil, err
	}

	return s.blobs.BlobReader(digest)
}

func (s *repositoryService) DeleteBlob(repository, id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrBlobUnknown
	}

	return s.metadata.DeleteBlob(repository, digest)
}
