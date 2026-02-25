package fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
)

const (
	blobPathFormat   = "blobs/%s/%s/%s"
	uploadPathFormat = "uploads/%s"
)

// NewBlobStore returns a filesystem-based store.Blobs implementation.
// It will store all blobs under the baseDir argument.
func NewBlobStore(baseDir string) store.Blobs {
	return &blobStore{
		baseDir: baseDir,
	}
}

type blobStore struct {
	baseDir string
}

func (s *blobStore) AllBlobs() iter.Seq2[digest.Digest, error] {
	return func(yield func(digest.Digest, error) bool) {
		path := filepath.Join(s.baseDir, "blobs")
		s.allBlobs(path, yield)
	}
}

func (s *blobStore) allBlobs(path string, yield func(digest.Digest, error) bool) {
	dir, err := os.Open(path)
	if err != nil {
		yield("", err)
		return
	}
	defer dir.Close() // nolint: errcheck

	for {
		files, err := dir.ReadDir(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			yield("", err)
			return
		}

		for _, file := range files {
			fullname := filepath.Join(path, file.Name())
			if file.IsDir() {
				s.allBlobs(fullname, yield)
				continue
			}

			id, err := s.pathToDigest(fullname)
			if err != nil {
				yield("", err)
				return
			}

			if !yield(id, nil) {
				return
			}
		}
	}
}

// StatBlob returns basic file info about the blob with the given digest.
func (s *blobStore) StatBlob(id digest.Digest) (*store.BlobInfo, error) {
	path := s.digestToPath(id)
	return s.stat(path)
}

// GetBlob returns the blob at the given path. Intended for smaller blobs that
// must be fully read into memory server-side, like manifests.
func (s *blobStore) GetBlob(id digest.Digest) ([]byte, error) {
	path := s.digestToPath(id)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(f)
}

// BlobReader returns an io.Reader that can be used to read a blob in a streaming fashion.
func (s *blobStore) BlobReader(id digest.Digest) (io.Reader, error) {
	path := s.digestToPath(id)
	return os.Open(path)
}

func (s *blobStore) BlobWriter(id digest.Digest) (io.Writer, error) {
	path := s.digestToPath(id)

	base := filepath.Dir(path)
	err := os.MkdirAll(base, 0755)
	if err != nil {
		return nil, err
	}

	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY, os.ModeAppend)
}

// PutBlob writes content to the given path. Intended for smaller blobs that
// must be fully read into memory server-side, like manifests.
// Put does not append and always writes the entire blob.
func (s *blobStore) PutBlob(id digest.Digest, content []byte) error {
	path := s.digestToPath(id)

	base := filepath.Dir(path)
	err := os.MkdirAll(base, 0755)
	if err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	_, err = io.Copy(f, bytes.NewBuffer(content))
	return err
}

// DeleteBlob removes a blob from the blob store.
func (s *blobStore) DeleteBlob(id digest.Digest) error {
	path := s.digestToPath(id)
	return os.Remove(path)
}

// StatBlob returns basic file info about the upload with the given UUID.
func (s *blobStore) StatUpload(id uuid.UUID) (*store.BlobInfo, error) {
	path := s.uuidToPath(id)
	return s.stat(path)
}

// InitUpload prepares the blob store to start an upload. In most implementations,
// it will create an empty file on the blob store that will later be appended.
func (s *blobStore) InitUpload(id uuid.UUID) error {
	path := s.uuidToPath(id)

	base := filepath.Dir(path)
	err := os.MkdirAll(base, 0755)
	if err != nil {
		return err
	}

	_, err = os.Create(path)
	return err
}

// UploadWriter returns an io.Writer to write to an initialized upload.
// Uploads are always uploaded in order andappended to. If an upload fails or must be truncated,
// a new session must be started instead.
func (s *blobStore) UploadWriter(id uuid.UUID) (io.WriteCloser, error) {
	path := s.uuidToPath(id)
	return os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
}

// CloseUpload finishes an upload and makes its contents accessible in the blob store by its digest.
// In some implementations, this may effectively be a rename.
func (s *blobStore) CloseUpload(id uuid.UUID, digest digest.Digest) error {
	uploadPath := s.uuidToPath(id)
	blobPath := s.digestToPath(digest)

	base := filepath.Dir(blobPath)
	err := os.MkdirAll(base, 0755)
	if err != nil {
		return err
	}

	return os.Rename(uploadPath, blobPath)
}

// DeleteUpload removes an upload from the store.
// Intended for cleaning up expired or failed uploads.
func (s *blobStore) DeleteUpload(id uuid.UUID) error {
	path := s.uuidToPath(id)
	return os.Remove(path)
}

func (s *blobStore) digestToPath(id digest.Digest) string {
	return filepath.Join(
		s.baseDir,
		fmt.Sprintf(blobPathFormat, id.Algorithm(), id.Encoded()[0:2], id.Encoded()),
	)
}

func (s *blobStore) pathToDigest(path string) (digest.Digest, error) {
	parts := strings.Split(path, string(os.PathSeparator))
	return digest.Parse(fmt.Sprintf("%s:%s", parts[len(parts)-3], parts[len(parts)-1]))
}

func (s *blobStore) uuidToPath(id uuid.UUID) string {
	return filepath.Join(
		s.baseDir,
		fmt.Sprintf(uploadPathFormat, id.String()),
	)
}

func (s *blobStore) stat(path string) (*store.BlobInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &store.BlobInfo{
		Name: path,
		Size: info.Size(),
	}, nil
}
