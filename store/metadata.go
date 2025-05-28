package store

import (
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

type (
	Metadata interface {
		GetRepository(name string) error
		CreateRepository(name string) error
		DeleteRepository(name string) error

		GetBlob(name string, digest digest.Digest) (string, error)
		PutBlob(name string, digest digest.Digest) error
		DeleteBlob(name string, digest digest.Digest) error

		GetManifest(name string, digest digest.Digest) (*ManifestMetadata, error)
		PutManifest(name string, digest digest.Digest, meta *ManifestMetadata) error
		DeleteManifest(name string, digest digest.Digest) error

		ListTags(name string, count int, last string) ([]string, error)
		GetTag(name, tag string) (digest.Digest, error)
		PutTag(name, tag string, digest digest.Digest) error
		DeleteTag(name, tag string) error

		ListReferrers(name string, digest digest.Digest) ([]digest.Digest, error)

		GetUploadSession(name string, id string) (*UploadSession, error)
		PutUploadSession(name string, session *UploadSession) error
		DeleteUploadSession(name string, id string) error
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
		HashState []byte
	}
)
