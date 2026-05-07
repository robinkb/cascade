package store

import (
	"errors"
	"io"
	"iter"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/pkg/pbutil"
	"github.com/robinkb/cascade/registry/store/storepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrRepositoryNotFound = errors.New("repository not found")
	ErrRepositoryExists   = errors.New("repository with the given name already exists")
	ErrBlobNotFound       = errors.New("blob not found")
	ErrLinkNotFound       = errors.New("blob not linked in repository")
	ErrManifestNotFound   = errors.New("manifest not found")
	ErrTagNotFound        = errors.New("tag not found")
	ErrUploadNotFound     = errors.New("upload session not found")

	ErrManifestInvalid         = errors.New("manifest invalid") // paired with more detailed errors below
	ErrManifestConfigNotFound  = errors.New("blob referenced in manifest config descriptor not found")
	ErrManifestLayerNotFound   = errors.New("blob referenced in manifest layers not found")
	ErrManifestImageNotFound   = errors.New("manifest referenced in image index not found")
	ErrManifestSubjectNotFound = errors.New("subject referenced in manifest not found")

	ErrLinkInUse     = errors.New("blob cannot be unlinked because it is in use")
	ErrManifestInUse = errors.New("manifest cannot be deleted because it is in use")
)

type (
	Metadata interface {
		// ListRepository returns a list of repository names.
		ListRepositories(count int, last string) ([]string, error)
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
		// Snapshot writes a snapshot of the Metadata store to the given Writer.
		Snapshot(w io.Writer) error
		// Restore reads a snapshot of the Metadata store from the given Reader.
		Restore(r io.Reader) error
	}

	Repository interface {
		GetLink(id digest.Digest) error
		PutLink(id digest.Digest) error
		DeleteLink(id digest.Digest) error

		GetManifest(id digest.Digest) (Manifest, error)
		PutManifest(id digest.Digest, meta Manifest, refs References) error
		DeleteManifest(id digest.Digest) ([]digest.Digest, error)

		ListReferrers(subject digest.Digest) ([]digest.Digest, error)

		ListTags(count int, last string) ([]string, error)
		GetTag(tag string) (digest.Digest, error)
		PutTag(tag string, digest digest.Digest) ([]digest.Digest, error)
		DeleteTag(tag string) ([]digest.Digest, error)

		GetUploadSession(id uuid.UUID) (*UploadSession, error)
		PutUploadSession(session *UploadSession) error
		DeleteUploadSession(id uuid.UUID) error
	}
)

// Manifest represents the metadata of a manifest that is stored in the MetadataStore.
type Manifest struct {
	Annotations  map[string]string
	ArtifactType string
	MediaType    string
	Size         int64
}

func (m *Manifest) Marshal() []byte {
	obj := storepb.Manifest_builder{
		Annotations:  m.Annotations,
		ArtifactType: &m.ArtifactType,
		MediaType:    &m.MediaType,
		Size:         &m.Size,
	}.Build()
	return pbutil.MustMarshal(obj)
}

func (m *Manifest) Unmarshal(b []byte) {
	obj := new(storepb.Manifest)
	pbutil.MustUnmarshal(b, obj)

	m.Annotations = obj.GetAnnotations()
	m.ArtifactType = obj.GetArtifactType()
	m.MediaType = obj.GetMediaType()
	m.Size = obj.GetSize()
}

// References defines the various ways in which a manifest can point to other objects in the registry.
// These are mostly used to establish links between objects for garbage collection.
type References struct {
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

func (r *References) Marshal() []byte {
	b := storepb.References_builder{
		Config:  proto.String(r.Config.String()),
		Subject: proto.String(r.Subject.String()),
	}
	for _, id := range r.Layers {
		b.Layers = append(b.Layers, id.String())
	}
	for _, id := range r.Manifests {
		b.Manifests = append(b.Manifests, id.String())
	}
	return pbutil.MustMarshal(b.Build())
}

func (r *References) Unmarshal(b []byte) {
	obj := new(storepb.References)
	pbutil.MustUnmarshal(b, obj)

	r.Config = digest.Digest(obj.GetConfig())
	r.Subject = digest.Digest(obj.GetSubject())

	layers := obj.GetLayers()
	r.Layers = make([]digest.Digest, len(layers))
	for _, id := range layers {
		r.Layers = append(r.Layers, digest.Digest(id))
	}

	manifests := obj.GetManifests()
	r.Manifests = make([]digest.Digest, len(manifests))
	for _, id := range manifests {
		r.Manifests = append(r.Manifests, digest.Digest(id))
	}
}

type UploadSession struct {
	ID        uuid.UUID
	StartDate time.Time
	HashState []byte
}

func (s *UploadSession) Marshal() []byte {
	obj := storepb.UploadSession_builder{
		Id:        s.ID.Bytes(),
		StartDate: timestamppb.New(s.StartDate),
		HashState: s.HashState,
	}.Build()
	return pbutil.MustMarshal(obj)
}

func (s *UploadSession) Unmarshal(b []byte) {
	obj := new(storepb.UploadSession)
	pbutil.MustUnmarshal(b, obj)

	s.ID = uuid.UUID(obj.GetId())
	s.StartDate = obj.GetStartDate().AsTime()
	s.HashState = obj.GetHashState()
}
