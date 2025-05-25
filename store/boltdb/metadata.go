package boltdb

import (
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
)

func NewMetadataStore() *metadataStore {
	return new(metadataStore)
}

type metadataStore struct {
	store.Metadata
}

func (s *metadataStore) GetBlob(repository string, digest digest.Digest) (string, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) PutBlob(repository string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteBlob(repository string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) GetManifest(repository string, digest digest.Digest) (*store.ManifestMetadata, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) PutManifest(repository string, digest digest.Digest, meta *store.ManifestMetadata) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteManifest(repository string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) ListTags(repository string, count int, last string) ([]string, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) GetTag(repository string, tag string) (digest.Digest, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) PutTag(repository string, tag string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteTag(repository string, tag string) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) ListReferrers(repository string, digest digest.Digest) ([]digest.Digest, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) GetUploadSession(repository string, id string) (*store.UploadSession, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) PutUploadSession(repository string, session *store.UploadSession) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteUploadSession(repository string, id string) error {
	panic("not implemented") // TODO: Implement
}
