package store

import (
	"errors"
	"io"
	"iter"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

var (
	ErrRepositoryNotFound     = errors.New("repository not found")
	ErrRepositoryExists       = errors.New("repository with the given name already exists")
	ErrRepositoryBlobNotFound = errors.New("blob not found in repository")

	ErrManifestNotFound = errors.New("manifest not found")

	ErrBlobInUse               = errors.New("blob cannot be deleted because it is in use")
	ErrManifestInUse           = errors.New("manifest cannot be deleted because it is in use")
	ErrManifestInvalid         = errors.New("manifest invalid") // usually paired with more detailed errors below
	ErrManifestConfigNotFound  = errors.New("blob referenced in manifest config descriptor not found")
	ErrManifestLayerNotFound   = errors.New("blob referenced in manifest layers not found")
	ErrManifestImageNotFound   = errors.New("manifest referenced in image index not found")
	ErrManifestSubjectNotFound = errors.New("subject referenced in manifest not found")
)

type (
	Metadata interface {
		// GetRepository verifies that a repository with the given name exists in the store.
		// If a repository does not exists, it returns ErrRepositoryNotFound.
		GetRepository(name string) (Repository, error)
		// CreateRepository creates a new repository in the store.
		// If a repository with the given name already exists, it returns ErrRepositoryExists.
		CreateRepository(name string) (Repository, error)
		// DeleteRepository deletes an existing repository and all of its resources from the store.
		// If a repository with the given name does not exist, it returns ErrRepositoryNotFound.
		DeleteRepository(name string) error
		// Blobs iterates over all blobs in the Metadata store.
		Blobs() iter.Seq[digest.Digest]
		// Snapshot writes a snapshot of the MetadataStore to the given Writer.
		Snapshot(w io.Writer) error
		// Restore reads a snapshot of the MetadataStore from the given Reader.
		Restore(r io.Reader) error
	}

	// TODO: DeleteManifest and DeleteTag should probably return a []digest.Digest
	// indicating which blobs were deleted from the store due to garbage collection.
	// The business layer is then responsible for actually deleting them from the blob store.
	// Any errors in doing so should only be logged as internal errors; the client
	// should not be bothered with it. All they need to know is if the manifest or tag got deleted.
	Repository interface {
		ListBlobs() ([]digest.Digest, error)
		GetBlob(digest digest.Digest) error
		PutBlob(digest digest.Digest) error
		DeleteBlob(digest digest.Digest) error

		GetManifest(digest digest.Digest) (Manifest, error)
		PutManifest(digest digest.Digest, meta Manifest, refs References) error
		DeleteManifest(digest digest.Digest) ([]digest.Digest, error)

		ListReferrers(digest digest.Digest) ([]digest.Digest, error)

		ListTags(count int, last string) ([]string, error)
		GetTag(tag string) (digest.Digest, error)
		PutTag(tag string, digest digest.Digest) error
		DeleteTag(tag string) ([]digest.Digest, error)

		GetUploadSession(id string) (*UploadSession, error)
		PutUploadSession(session *UploadSession) error
		DeleteUploadSession(id string) error
	}

	// Manifest represents the metadata of a manifest that is stored in the MetadataStore.
	Manifest struct {
		Annotations  map[string]string
		ArtifactType string
		MediaType    string
		Size         int64
	}

	// References defines the various ways in which a manifest can point to other objects in the registry.
	// These are mostly used to establish links between objects for garbage collection.
	// TODO: According to the OCI Distribution spec, manifest uploads MAY be rejected if any of the referenced
	// blobs or manifests do not exist in the registry. Doing so would allow greater guarantees of
	// no stale references existing in the metadata store, and may eliminate the need of having an additional
	// mechanism to verify that there are no stale references.
	References struct {
		// Config references a configuration object for a container like in an OCI Image Manifest.
		// It is used for tracking links from image manifests to a blob for garbage collection.
		Config digest.Digest
		// Layers is a slice of digests pointing to blobs like in an OCI Image Manifest.
		// It is used for tracking links from image manifests to blobs for garbage collection.
		Layers []digest.Digest
		// Manifests is a slice of digests pointing to other manifests like in an OCI Image Index.
		// It is used for tracking links from image index manifests to other manifests for garbage collection.
		Manifests []digest.Digest
		// Subject is a digest pointing to another manifest as used by the Referrers API.
		// Besides being used for the Referrers API, it is also used for garbage collection.
		Subject digest.Digest
	}

	UploadSession struct {
		ID        uuid.UUID
		StartDate time.Time
		HashState []byte
	}
)
