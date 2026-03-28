package repository

import (
	"errors"
	"io"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

func (s *repositoryService) StatBlob(id string) (*store.BlobInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	err = s.repo.GetBlob(digest)
	if err != nil {
		if errors.Is(err, store.ErrBlobNotFound) {
			err = ErrBlobUnknown
		}
		return nil, err
	}

	return s.blobs.StatBlob(digest)
}

func (s *repositoryService) GetBlob(id string) (io.Reader, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	err = s.repo.GetBlob(digest)
	if err != nil {
		if errors.Is(err, store.ErrBlobNotFound) {
			err = ErrBlobUnknown
		}
		return nil, err
	}

	return s.blobs.BlobReader(digest)
}

func (s *repositoryService) DeleteBlob(id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrBlobUnknown
	}

	return s.repo.DeleteBlob(digest)
}
