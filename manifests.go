package cascade

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func (s *registryService) StatManifest(repository, id string) (*FileInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	_, err = s.metadata.GetManifest(repository, digest)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	info, err := s.blobs.StatBlob(digest)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrManifestUnknown
	}

	return info, err
}

func (s *registryService) GetManifest(repository, id string) (*ManifestMetadata, []byte, error) {
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

func (s *registryService) PutManifest(repository, reference string, content []byte) (digest.Digest, error) {
	var subject digest.Digest

	digest, err := digest.Parse(reference)
	if err != nil {
		return "", ErrDigestInvalid
	}

	var manifest v1.Manifest
	err = json.Unmarshal(content, &manifest)
	if err != nil {
		return "", ErrManifestInvalid
	}

	if manifest.Subject != nil {
		subject = manifest.Subject.Digest
	}

	path := fmt.Sprintf("blobs/%s/%s/%s", digest.Algorithm(), digest.Encoded()[0:2], digest.Encoded())

	err = s.blobs.Put(path, content)
	if err != nil {
		return "", err
	}

	meta := &ManifestMetadata{
		Annotations:  manifest.Annotations,
		ArtifactType: manifest.ArtifactType,
		MediaType:    manifest.MediaType,
		Path:         path,
		Subject:      subject,
		Size:         int64(len(content)),
	}

	if meta.ArtifactType == "" && manifest.MediaType == v1.MediaTypeImageManifest {
		meta.ArtifactType = manifest.Config.MediaType
	}

	err = s.metadata.PutManifest(repository, digest, meta)

	return subject, err
}

func (s *registryService) DeleteManifest(repository, id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrManifestUnknown
	}

	meta, err := s.metadata.GetManifest(repository, digest)
	if err != nil {
		return ErrManifestUnknown
	}

	// TODO: Shouldn't do this. Manifests could be shared between repos.
	err = s.blobs.Delete(meta.Path)
	if err != nil {
		return err
	}

	return s.metadata.DeleteManifest(repository, digest)
}
