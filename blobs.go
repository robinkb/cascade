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

	path, err := s.metadata.GetBlob(repository, digest)
	if err != nil {
		return nil, err
	}

	info, err := s.blobs.Stat(path)
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

	path, err := s.metadata.GetBlob(repository, digest)
	if err != nil {
		return nil, err
	}

	return s.blobs.Reader(path)
}
