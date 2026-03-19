package store

import (
	"errors"
	"io"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

var (
	ErrRepositoryNotFound     = errors.New("repository not found")
	ErrRepositoryExists       = errors.New("repository with the given name already exists")
	ErrRepositoryBlobNotFound = errors.New("blob not found in repository")
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
		// Blobs() iter.Seq[digest.Digest]
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

		GetManifest(digest digest.Digest) (*ManifestMetadata, error)
		PutManifest(digest digest.Digest, meta *ManifestMetadata) error
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

	// ManifestMetadata represents the metadata of a manifest that is stored in the MetadataStore.
	ManifestMetadata struct {
		Annotations  map[string]string
		ArtifactType string
		MediaType    string
		Subject      digest.Digest
		Size         int64
	}

	UploadSession struct {
		ID        uuid.UUID
		StartDate time.Time
		HashState []byte
	}
)
