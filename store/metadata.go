package store

import (
	"io"
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

	// Snapshotter snapshots and restores a Metadata store.
	Snapshotter interface {
		// Snapshot writes a snapshot of the MetadataStore to the given Writer.
		Snapshot(w io.Writer) error
		// Restore reads a snapshot of the MetadataStore from the given Reader.
		Restore(r io.Reader) error
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
