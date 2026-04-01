package repository

import (
	"io"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

type (
	Service interface {
		StatBlob(digest string) (*store.BlobInfo, error)
		GetBlob(digest string) (io.Reader, error)
		DeleteBlob(digest string) error

		StatManifest(reference string) (*store.BlobInfo, error)
		GetManifest(reference string) (*store.Manifest, []byte, error)
		PutManifest(reference string, content []byte) (digest.Digest, error)
		DeleteManifest(reference string) error

		ListTags(count int, from string) ([]string, error)
		GetTag(tag string) (string, error)
		PutTag(tag, digest string) error
		DeleteTag(tag string) error

		ListReferrers(digest string, opts *ListReferrersOptions) (*Referrers, error)

		InitUpload() (*store.UploadSession, error)
		StatUpload(sessionID string) (*store.BlobInfo, error)
		AppendUpload(sessionID string, r io.Reader, offset int64) error
		CloseUpload(id, digest string) error
	}
)

func New(blobs store.Blobs, repo store.Repository) Service {
	return &repositoryService{
		blobs: blobs,
		repo:  repo,
	}
}

type repositoryService struct {
	blobs store.Blobs
	repo  store.Repository
}
