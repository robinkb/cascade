package store

import (
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

type (
	// TODO: This interface currently does not cover managing repositories.
	// In real-world stores, where creating a repository is not so simple,
	// this tends to be problematic. There must also be a way to delete repositories.
	// I do like having the option of just creating repos on the fly, though...
	Metadata interface {
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

	// ManifestMetadata represents the metadata of a manifest that is stored in the MetadataStore.
	ManifestMetadata struct {
		Annotations  map[string]string
		ArtifactType string
		MediaType    string
		Subject      digest.Digest
		Size         int64
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
