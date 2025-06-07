package cluster

import (
	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
)

// Blob operations
type initUpload struct {
	Id        uint64
	SessionID uuid.UUID
}

func (o *initUpload) ID() uint64 { return o.Id }

// Metadata operations
type createRepository struct {
	Id   uint64
	Name string
}

func (o *createRepository) ID() uint64 { return o.Id }

type deleteRepository struct {
	Id   uint64
	Name string
}

func (o *deleteRepository) ID() uint64 { return o.Id }

type putBlob struct {
	Id     uint64
	Name   string
	Digest digest.Digest
}

func (o *putBlob) ID() uint64 { return o.Id }

type deleteBlob struct {
	Id     uint64
	Name   string
	Digest digest.Digest
}

func (o *deleteBlob) ID() uint64 { return o.Id }

type putManifest struct {
	Id     uint64
	Name   string
	Digest digest.Digest
	Meta   *store.ManifestMetadata
}

func (o *putManifest) ID() uint64 { return o.Id }

type deleteManifest struct {
	Id     uint64
	Name   string
	Digest digest.Digest
}

func (o *deleteManifest) ID() uint64 { return o.Id }

type putTag struct {
	Id     uint64
	Name   string
	Tag    string
	Digest digest.Digest
}

func (o *putTag) ID() uint64 { return o.Id }

type deleteTag struct {
	Id   uint64
	Name string
	Tag  string
}

func (o *deleteTag) ID() uint64 { return o.Id }

type putUploadSession struct {
	Id      uint64
	Name    string
	Session *store.UploadSession
}

func (o *putUploadSession) ID() uint64 { return o.Id }

type deleteUploadSession struct {
	Id        uint64
	Name      string
	SessionID string
}

func (o *deleteUploadSession) ID() uint64 { return o.Id }
