package repository

import (
	"io"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
)

type (
	RepositoryService interface {
		StatBlob(repository, digest string) (*store.BlobInfo, error)
		GetBlob(repository, digest string) (io.Reader, error)
		DeleteBlob(repository, digest string) error

		StatManifest(repository, reference string) (*store.BlobInfo, error)
		GetManifest(repository, reference string) (*store.ManifestMetadata, []byte, error)
		PutManifest(repository, reference string, content []byte) (digest.Digest, error)
		DeleteManifest(repository, reference string) error

		ListTags(repository string, count int, from string) ([]string, error)
		GetTag(repository, tag string) (string, error)
		PutTag(repository, tag, digest string) error
		DeleteTag(repository, tag string) error

		ListReferrers(repository, digest string, opts *ListReferrersOptions) (*Referrers, error)

		InitUpload(repository string) (*store.UploadSession, error)
		StatUpload(repository, sessionID string) (*store.BlobInfo, error)
		AppendUpload(repository, sessionID string, r io.Reader, offset int64) error
		CloseUpload(repository, id, digest string) error
	}
)

func NewRepositoryService(metadata store.Metadata, blobs store.Blobs) *repositoryService {
	return &repositoryService{
		metadata: metadata,
		blobs:    blobs,
	}
}

type repositoryService struct {
	blobs    store.Blobs
	metadata store.Metadata
}
