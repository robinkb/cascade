package cascade

import (
	"errors"
	"io"

	"github.com/opencontainers/go-digest"
)

func (s *registryService) StatBlob(repository, id string) (*FileInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	_, err = s.metadata.GetBlob(repository, digest)
	if err != nil {
		return nil, err
	}

	info, err := s.blobs.StatBlob(digest)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUnknown
	}

	return info, err
}

func (s *registryService) GetBlob(repository, id string) (io.Reader, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	_, err = s.metadata.GetBlob(repository, digest)
	if err != nil {
		return nil, err
	}

	return s.blobs.BlobReader(digest)
}

func (s *registryService) DeleteBlob(repository, id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrBlobUnknown
	}

	return s.metadata.DeleteBlob(repository, digest)
}
