package cascade

type (
	RegistryService interface {
		StatBlob(repository, digest string) (*FileInfo, error)
		GetBlob(repository, digest string) ([]byte, error)

		StatManifest(repository, reference string) (*FileInfo, error)
		GetManifest(repository, reference string) ([]byte, error)
		PutManifest(repository, reference string, content []byte) error
		DeleteManifest(repository, reference string) error

		GetTag(repository, tag string) (string, error)
		PutTag(repository, tag, digest string) error
		DeleteTag(repository, tag string) error

		InitUpload(repository string) *UploadSession
		StatUpload(repository, sessionID string) (*FileInfo, error)
		AppendUpload(repository, sessionID string, content []byte, offset int64) error
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

func NewRegistryService(store RegistryStore) *registryService {
	return &registryService{
		store:        store,
		sessionStore: make(map[string]map[string]bool),
	}
}

type registryService struct {
	store        RegistryStore
	sessionStore map[string]map[string]bool
}
