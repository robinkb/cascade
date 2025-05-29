package raft

import (
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
)

func NewMetadataStore(store store.Metadata) store.Metadata {
	return &metadataStore{
		store: store,
	}
}

type metadataStore struct {
	store store.Metadata
}

func (s *metadataStore) GetRepository(name string) error {
	return s.store.GetRepository(name)
}

func (s *metadataStore) CreateRepository(name string) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteRepository(name string) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) GetBlob(name string, digest digest.Digest) (string, error) {
	return s.store.GetBlob(name, digest)
}

func (s *metadataStore) PutBlob(name string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteBlob(name string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) GetManifest(name string, digest digest.Digest) (*store.ManifestMetadata, error) {
	return s.store.GetManifest(name, digest)
}

func (s *metadataStore) PutManifest(name string, digest digest.Digest, meta *store.ManifestMetadata) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteManifest(name string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) ListTags(name string, count int, last string) ([]string, error) {
	return s.store.ListTags(name, count, last)
}

func (s *metadataStore) GetTag(name string, tag string) (digest.Digest, error) {
	return s.store.GetTag(name, tag)
}

func (s *metadataStore) PutTag(name string, tag string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteTag(name string, tag string) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) ListReferrers(name string, digest digest.Digest) ([]digest.Digest, error) {
	return s.store.ListReferrers(name, digest)
}

func (s *metadataStore) GetUploadSession(name string, id string) (*store.UploadSession, error) {
	return s.store.GetUploadSession(name, id)
}

func (s *metadataStore) PutUploadSession(name string, session *store.UploadSession) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteUploadSession(name string, id string) error {
	panic("not implemented") // TODO: Implement
}
