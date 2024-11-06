package cascade

import "io"

type (
	RegistryService interface {
		StatBlob(repository, digest string) (*FileInfo, error)
		GetBlob(repository, digest string) (io.Reader, error)

		StatManifest(repository, reference string) (*FileInfo, error)
		GetManifest(repository, reference string) (*Manifest, error)
		PutManifest(repository, reference string, content []byte) error
		DeleteManifest(repository, reference string) error

		ListTags(repository string) ([]string, error)
		GetTag(repository, tag string) (string, error)
		PutTag(repository, tag, digest string) error
		DeleteTag(repository, tag string) error

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
		ID, Location string
	}
)

// TODO: Modify to accept MetadataStore and BlobStore when MetadataStore is implemented.
func NewRegistryService(store RegistryStore) *registryService {
	return &registryService{
		store:        store,
		metadata:     NewInMemoryMetadataStore(),
		blobs:        NewInMemoryBlobStore(),
		sessionStore: make(map[string]map[string]bool),
	}
}

type registryService struct {
	store        RegistryStore
	blobs        BlobStore
	metadata     MetadataStore
	sessionStore map[string]map[string]bool
}
