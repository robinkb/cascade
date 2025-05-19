package store

import (
	"io"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

type (
	// Blobs defines the interface for storing the actual data of the registry.
	// Implementations of this interface are responsible for deciding how data is persisted.
	// Blobs must be retrievable by their digest, and uploads by their session ID.
	Blobs interface {
		// StatBlob returns basic file info about the blob with the given digest.
		StatBlob(id digest.Digest) (*BlobInfo, error)
		// GetBlob returns the blob at the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		GetBlob(id digest.Digest) ([]byte, error)
		// BlobReader returns an io.Reader that can be used to read a blob in a streaming fashion.
		BlobReader(id digest.Digest) (io.Reader, error)
		// PutBlob writes content to the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		// Put does not append and always writes the entire blob.
		PutBlob(id digest.Digest, content []byte) error
		// DeleteBlob removes a blob from the blob store.
		DeleteBlob(id digest.Digest) error

		// StatBlob returns basic file info about the upload with the given UUID.
		StatUpload(id uuid.UUID) (*BlobInfo, error)
		// InitUpload prepares the blob store to start an upload. In most implementations,
		// it will create an empty file on the blob store that will later be appended.
		InitUpload(id uuid.UUID) error
		// UploadWriter returns an io.Writer to write to an initialized upload.
		// Uploads are always uploaded in order andappended to. If an upload fails or must be truncated,
		// a new session must be started instead.
		UploadWriter(id uuid.UUID) (io.Writer, error)
		// CloseUpload finishes an upload and makes its contents accessible in the blob store by its digest.
		// In some implementations, this may effectively be a rename.
		CloseUpload(id uuid.UUID, digest digest.Digest) error
		// DeleteUpload removes an upload from the store.
		// Intended for cleaning up expired or failed uploads.
		DeleteUpload(id uuid.UUID) error
	}

	// BlobInfo contains the basic information of a blob.
	BlobInfo struct {
		Name string
		Size int64
	}
)
