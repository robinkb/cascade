package scratch

import (
	"context"
)

type (
	Registry interface {
		// Repository can do validation that is common to all repositories, like on the name.
		Repository(ctx context.Context, name string) (RepositoryManager, error)
	}

	RepositoryManager interface {
		// These could do validation on the digest and such?
		Blob(digest string) (BlobManager, error)
		Manifest(digest string) (ManifestManager, error)
		Tag(name string) (TagManager, error)
		UploadSession() (UploadSessionManager, error)
		Upload(sessionID string) (UploadManager, error)
	}

	BlobManager interface {
		Stat() (*FileInfo, error)
		Get() ([]byte, error)
		Delete() error
	}

	ManifestManager interface {
		Stat() (*FileInfo, error)
		Get() ([]byte, error)
		Put(content []byte) error
		Delete() error
	}

	TagManager interface {
		Get() (string, error)
		Put(digest string) error
		Delete() error
	}

	UploadSessionManager interface {
		Init() (*UploadSession, error)
	}

	UploadManager interface {
		Stat() (*FileInfo, error)
		Append(content []byte) error
		Close(digest string) error
	}

	FileInfo      struct{}
	UploadSession struct{}
)
