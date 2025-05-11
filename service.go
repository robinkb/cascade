package cascade

import (
	"io"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

type (
	RegistryService interface {
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

		InitUpload(repository string) *UploadSession
		StatUpload(repository, sessionID string) (*FileInfo, error)
		AppendUpload(repository, sessionID string, r io.Reader, offset int64) error
		CloseUpload(repository, id, digest string) error
	}

	// TODO: Could refactor to this:
	// RegistryService interface {
	// 	Repository(ctx context.Context, name string) RepositoryService
	// }

	// RepositoryService interface {
	// 	StatBlob(digest string) (*FileInfo, error)
	// 	GetBlob(digest string) ([]byte, error)
	// 	StatManifest(reference string) (*FileInfo, error)
	// 	GetManifest(reference string) ([]byte, error)
	// 	PutManifest(reference string, content []byte) error
	// 	DeleteManifest(reference string) error
	// 	InitUpload() *UploadSession
	// 	StatUpload(sessionID string) (*FileInfo, error)
	// 	WriteUpload(sessionID string, content []byte) error
	// 	CloseUpload(id, digest string) error
	// }

	UploadSession struct {
		ID uuid.UUID
		// TODO: This should not be here, as it's an HTTP implementation detail.
		Location string
		// TODO: Will go away with the blob store refactor, as uploads
		// are saved based on the session ID.
		BlobPath  string
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
