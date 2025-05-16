package cascade

import (
	"errors"
	"io"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

var (
	ErrFileNotFound = errors.New("file not found")
)

type (
	// TODO: This interface currently does not cover managing repositories.
	// In real-world stores, where creating a repository is not so simple,
	// this tends to be problematic. There must also be a way to delete repositories.
	// I do like having the option of just creating repos on the fly, though...
	MetadataStore interface {
		GetBlob(repository string, digest digest.Digest) (string, error)
		PutBlob(repository string, digest digest.Digest) error
		DeleteBlob(repository string, digest digest.Digest) error

		GetManifest(repository string, digest digest.Digest) (*ManifestMetadata, error)
		PutManifest(repository string, digest digest.Digest, meta *ManifestMetadata) error
		DeleteManifest(repository string, digest digest.Digest) error

		ListTags(repository string, count int, last string) ([]string, error)
		GetTag(repository, tag string) (digest.Digest, error)
		PutTag(repository, tag string, digest digest.Digest) error
		DeleteTag(repository, tag string) error

		ListReferrers(repository string, digest digest.Digest) ([]digest.Digest, error)

		GetUploadSession(repository string, id string) (*UploadSession, error)
		PutUploadSession(repository string, session *UploadSession) error
		DeleteUploadSession(repository string, id string) error
	}

	// BlobStore defines the interface for storing the actual data of the registry.
	// Implementations of this interface are responsible for deciding how data is persisted.
	// Blobs must be retrievable by their digest, and uploads by their session ID.
	BlobStore interface {
		// StatBlob returns basic file info about the blob with the given digest.
		StatBlob(id digest.Digest) (*FileInfo, error)
		// GetBlob returns the blob at the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		GetBlob(id digest.Digest) ([]byte, error)
		// BlobReader returns an io.Reader that can be used to read a blob in a streaming fashion.
		BlobReader(id digest.Digest) (io.Reader, error)
		// PutBlob writes content to the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		// Put does not append and always writes the entire blob.
		PutBlob(id digest.Digest, content []byte) error
		// DeleteBlob removes a blob from the blobstore.
		DeleteBlob(id digest.Digest) error

		// StatBlob returns basic file info about the upload with the given UUID.
		StatUpload(id uuid.UUID) (*FileInfo, error)
		// InitUpload prepares the BlobStore to start an upload. In most implementations,
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

	// Based (at least initially) on fs.FileInfo interface.
	FileInfo struct {
		Name string
		Size int64
	}

	// ManifestMetadata represents the metadata of a manifest that is stored in the MetadataStore.
	ManifestMetadata struct {
		Annotations  map[string]string
		ArtifactType string
		MediaType    string
		Subject      digest.Digest
		Size         int64
	}
)
