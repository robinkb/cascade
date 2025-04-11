package cascade

import (
	"errors"
	"io"

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
		PutBlob(repository string, digest digest.Digest, path string) error
		DeleteBlob(repository string, digest digest.Digest) error

		GetManifest(repository string, digest digest.Digest) (string, error)
		PutManifest(repository string, digest digest.Digest, meta *ManifestMetadata) error
		DeleteManifest(repository string, digest digest.Digest) error

		ListTags(repository string, count int, last string) ([]string, error)
		// TODO: Return digest.Digest instead of string?
		GetTag(repository, tag string) (string, error)
		// TODO: Accept digest.Digest instead of string?
		PutTag(repository, tag, digest string) error
		DeleteTag(repository, tag string) error

		// TODO: Rename to UploadSession
		GetUpload(repository string, id string) (*UploadSession, error)
		PutUpload(repository string, session *UploadSession) error
		DeleteUpload(repository string, id string) error
	}

	BlobStore interface {
		// Stat returns basic file info about the blob at the given path.
		Stat(path string) (*FileInfo, error)
		// Get returns the blob at the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		Get(path string) ([]byte, error)
		// Put writes content to the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		// Unlike Writer, Put does not append and always writes the entire blob.
		Put(path string, content []byte) error
		// Reader returns an io.Reader that can be used to read a blob.
		Reader(path string) (io.Reader, error)
		// Writer returns an io.Writer to write to a blob. Blobs are always appended to.
		// If a blob must be truncated, delete it first.
		Writer(path string) (io.Writer, error)
		// Delete removes the blob at the given path.
		Delete(path string) error
		// Move moves the blob from the source path to the destination path.
		// This may effectively be a rename on some backends.
		Move(sourcePath, destinationPath string) error
	}

	// Based (at least initially) on fs.FileInfo interface.
	FileInfo struct {
		Name string
		Size int64
	}

	// ManifestMetadata represents the metadata of a manifest that is stored in the MetadataStore.
	ManifestMetadata struct {
		Path      string
		MediaType string
	}
)
