package repository

import (
	"encoding/json"
	"errors"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade/registry/store"
)

func (s *repositoryService) StatManifest(id string) (*store.BlobInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	_, err = s.repo.GetManifest(digest)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	info, err := s.blobs.StatBlob(digest)
	if errors.Is(err, store.ErrBlobNotFound) {
		return nil, ErrManifestUnknown
	}

	return info, err
}

func (s *repositoryService) GetManifest(id string) (*store.Manifest, []byte, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, nil, ErrBlobUnknown
	}

	meta, err := s.repo.GetManifest(digest)
	if err != nil {
		if errors.Is(err, store.ErrManifestNotFound) {
			err = ErrManifestUnknown
		}
		return nil, nil, err
	}

	content, err := s.blobs.GetBlob(digest)
	if err != nil {
		return nil, nil, err
	}

	return &meta, content, nil
}

func (s *repositoryService) PutManifest(reference string, content []byte) (digest.Digest, error) {
	id, err := digest.Parse(reference)
	if err != nil {
		return "", ErrDigestInvalid
	}

	// Parse a manifest as a v1.Descriptor to access the MediaType field
	// to find out which type it is.
	// TODO: Read the Content-Type header instead, and pass the type into this method?
	// Should find out of Podman et al set the header correctly.
	var desc v1.Descriptor
	err = json.Unmarshal(content, &desc)
	if err != nil {
		return "", ErrManifestInvalid
	}

	var metadata store.Manifest
	var references store.References

	switch desc.MediaType {
	case v1.MediaTypeImageManifest:
		err = s.processImageManifest(content, &metadata, &references)
	case v1.MediaTypeImageIndex:
		err = s.processImageIndex(content, &metadata, &references)
	default:
		return "", ErrManifestInvalid
	}
	if err != nil {
		return "", ErrManifestInvalid
	}

	err = s.blobs.PutBlob(id, content)
	if err != nil {
		return "", err
	}

	err = s.repo.PutManifest(id, metadata, references)

	return references.Subject, err
}

func (s *repositoryService) processImageManifest(content []byte, metadata *store.Manifest, references *store.References) error {
	var manifest v1.Manifest
	err := json.Unmarshal(content, &manifest)
	if err != nil {
		return ErrManifestInvalid
	}

	references.Config = manifest.Config.Digest

	if len(manifest.Layers) > 0 {
		references.Layers = make([]digest.Digest, len(manifest.Layers))
		for i, layer := range manifest.Layers {
			references.Layers[i] = layer.Digest
		}
	}

	if manifest.Subject != nil {
		references.Subject = manifest.Subject.Digest
	}

	metadata.Annotations = manifest.Annotations
	metadata.ArtifactType = manifest.ArtifactType
	metadata.MediaType = manifest.MediaType
	metadata.Size = int64(len(content))

	if metadata.ArtifactType == "" && manifest.MediaType == v1.MediaTypeImageManifest {
		metadata.ArtifactType = manifest.Config.MediaType
	}

	return nil
}

func (s *repositoryService) processImageIndex(content []byte, metadata *store.Manifest, references *store.References) error {
	var index v1.Index
	err := json.Unmarshal(content, &index)
	if err != nil {
		return ErrManifestInvalid
	}

	if len(index.Manifests) > 0 {
		references.Manifests = make([]digest.Digest, len(index.Manifests))
		for i, manifest := range index.Manifests {
			references.Manifests[i] = manifest.Digest
		}
	}

	if index.Subject != nil {
		references.Subject = index.Subject.Digest
	}

	metadata.Annotations = index.Annotations
	metadata.ArtifactType = index.ArtifactType
	metadata.MediaType = index.MediaType
	metadata.Size = int64(len(content))

	return nil
}

func (s *repositoryService) DeleteManifest(id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrManifestUnknown
	}

	_, err = s.repo.GetManifest(digest)
	if err != nil {
		return ErrManifestUnknown
	}

	_, err = s.repo.DeleteManifest(digest)
	return err
}
