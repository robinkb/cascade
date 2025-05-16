package cascade

import (
	"io"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

type (
	RepositoryService interface {
		StatBlob(repository, digest string) (*FileInfo, error)
		GetBlob(repository, digest string) (io.Reader, error)
		DeleteBlob(repository, digest string) error

		StatManifest(repository, reference string) (*FileInfo, error)
		GetManifest(repository, reference string) (*ManifestMetadata, []byte, error)
		PutManifest(repository, reference string, content []byte) (digest.Digest, error)
		DeleteManifest(repository, reference string) error

		ListTags(repository string, count int, from string) ([]string, error)
		GetTag(repository, tag string) (string, error)
		PutTag(repository, tag, digest string) error
		DeleteTag(repository, tag string) error

		ListReferrers(repository, digest string, opts *ListReferrersOptions) (*Referrers, error)

		InitUpload(repository string) (*UploadSession, error)
		StatUpload(repository, sessionID string) (*FileInfo, error)
		AppendUpload(repository, sessionID string, r io.Reader, offset int64) error
		CloseUpload(repository, id, digest string) error
	}

	UploadSession struct {
		ID uuid.UUID
		// TODO: This should not be here, as it's an HTTP implementation detail.
		Location  string
		StartDate time.Time
		// TODO: Could we make this a hash.Hash and make it easier?
		HashState []byte
	}
)

func NewRegistryService(metadata MetadataStore, blobs BlobStore) *registryService {
	return &registryService{
		metadata: metadata,
		blobs:    blobs,
	}
}

type registryService struct {
	blobs    BlobStore
	metadata MetadataStore
}
