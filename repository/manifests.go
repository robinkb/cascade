package repository

import (
	"encoding/json"
	"errors"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry/store"
)

func (s *repositoryService) StatManifest(repository, id string) (*store.BlobInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	_, err = s.metadata.GetManifest(repository, digest)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	info, err := s.blobs.StatBlob(digest)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrManifestUnknown
	}

	return info, err
}

func (s *repositoryService) GetManifest(repository, id string) (*store.ManifestMetadata, []byte, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, nil, ErrBlobUnknown
	}

	meta, err := s.metadata.GetManifest(repository, digest)
	if err != nil {
		return nil, nil, ErrManifestUnknown
	}

	content, err := s.blobs.GetBlob(digest)
	if err != nil {
		return nil, nil, err
	}

	return meta, content, nil
}

func (s *repositoryService) PutManifest(repository, reference string, content []byte) (digest.Digest, error) {
	var subject digest.Digest

	digest, err := digest.Parse(reference)
	if err != nil {
		return "", ErrDigestInvalid
	}

	err = s.metadata.GetRepository(repository)
	if err != nil {
		if errors.Is(err, store.ErrRepositoryNotFound) {
			err = ErrNameUnknown
		}
		return "", err
	}

	var manifest v1.Manifest
	err = json.Unmarshal(content, &manifest)
	if err != nil {
		return "", ErrManifestInvalid
	}

	if manifest.Subject != nil {
		subject = manifest.Subject.Digest
	}

	err = s.blobs.PutBlob(digest, content)
	if err != nil {
		return "", err
	}

	meta := &store.ManifestMetadata{
		Annotations:  manifest.Annotations,
		ArtifactType: manifest.ArtifactType,
		MediaType:    manifest.MediaType,
		Subject:      subject,
		Size:         int64(len(content)),
	}

	if meta.ArtifactType == "" && manifest.MediaType == v1.MediaTypeImageManifest {
		meta.ArtifactType = manifest.Config.MediaType
	}

	err = s.metadata.PutManifest(repository, digest, meta)

	return subject, err
}

func (s *repositoryService) DeleteManifest(repository, id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrManifestUnknown
	}

	_, err = s.metadata.GetManifest(repository, digest)
	if err != nil {
		return ErrManifestUnknown
	}

	return s.metadata.DeleteManifest(repository, digest)
}
