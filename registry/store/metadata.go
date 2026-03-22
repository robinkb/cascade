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
	ErrManifestNotFound       = errors.New("manifest not found")
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

	Repository interface {
		ListBlobs() ([]digest.Digest, error)
		GetBlob(digest digest.Digest) error
		PutBlob(digest digest.Digest) error
		DeleteBlob(digest digest.Digest) error

		GetManifest(digest digest.Digest) (Manifest, error)
		// TODO: Must be amended to enable passing digests in the Layers, Config, and Subject fields,
		// as all of these fields establish links to other objects that must be accounted for
		// for garbage collection.
		// Obviously this must also be tested.
		// There's also the case of an image index, which may require a different method completely. Ugh.
		// Ultimately there should just be a "references []digest.Digest" argument.
		// These are all just digests pointing to blobs. Doesn't matter what they are for the metadata.
		// Extracting those digests is the job of the business layer above it.
		// Only the Subject field is a special case, because that's needed for the Referrers API.
		PutManifest(digest digest.Digest, meta Manifest) error
		DeleteManifest(digest digest.Digest) error

		ListTags(count int, last string) ([]string, error)
		GetTag(tag string) (digest.Digest, error)
		PutTag(tag string, digest digest.Digest) error
		DeleteTag(tag string) error

		ListReferrers(digest digest.Digest) ([]digest.Digest, error)

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
	References struct {
		// Layers is a slice of digests pointing to blobs like in an OCI Image Manifest.
		Layers []digest.Digest
		// Manifests is a slice of digests pointing to other manifests like in an OCI Image Index.
		Manifests []digest.Digest
		// Subject is a digest pointing to another manifest as used by the Referrers API.
		Subject digest.Digest
	}

	UploadSession struct {
		ID        uuid.UUID
		StartDate time.Time
		HashState []byte
	}
)
