package inmemory

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"os"
	"strings"
	"sync"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

const (
	blobPathFormat   = "blobs/%s/%s/%s"
	uploadPathFormat = "uploads/%s"
)

func NewBlobStore() store.Blobs {
	return &blobStore{
		store: make(map[string][]byte),
	}
}

type blobStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

func (s *blobStore) AllBlobs() iter.Seq2[digest.Digest, error] {
	return func(yield func(digest.Digest, error) bool) {
		for path := range s.store {
			id := s.pathToDigest(path)
			if !yield(id, nil) {
				return
			}
		}
	}
}

func (s *blobStore) StatBlob(id digest.Digest) (*store.BlobInfo, error) {
	path := s.digestToPath(id)
	return s.stat(path)
}

func (s *blobStore) GetBlob(id digest.Digest) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.digestToPath(id)
	data, ok := s.store[path]
	if !ok {
		return nil, store.ErrBlobNotFound
	}
	return data, nil
}

func (s *blobStore) BlobReader(id digest.Digest) (io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.digestToPath(id)
	return bytes.NewBuffer(s.store[path]), nil
}

func (s *blobStore) BlobWriter(id digest.Digest) (io.Writer, error) {
	path := s.digestToPath(id)
	return &writer{s, path}, nil
}

func (s *blobStore) PutBlob(id digest.Digest, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.digestToPath(id)
	s.store[path] = content

	return nil
}

func (s *blobStore) DeleteBlob(id digest.Digest) error {
	path := s.digestToPath(id)
	if _, ok := s.store[path]; !ok {
		return fmt.Errorf("%w: %s", store.ErrBlobNotFound, id)
	}
	return s.delete(path)
}

func (s *blobStore) StatUpload(id uuid.UUID) (*store.BlobInfo, error) {
	path := s.uuidToPath(id)
	return s.stat(path)
}

func (s *blobStore) InitUpload(id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.uuidToPath(id)
	s.store[path] = []byte{}

	return nil
}

func (s *blobStore) UploadWriter(id uuid.UUID) (io.WriteCloser, error) {
	path := s.uuidToPath(id)
	return &writer{s, path}, nil
}

func (s *blobStore) CloseUpload(id uuid.UUID, digest digest.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	uploadPath := s.uuidToPath(id)
	blobPath := s.digestToPath(digest)

	s.store[blobPath] = s.store[uploadPath]
	delete(s.store, uploadPath)
	return nil
}

func (s *blobStore) DeleteUpload(id uuid.UUID) error {
	path := s.uuidToPath(id)
	return s.delete(path)
}

func (s *blobStore) digestToPath(id digest.Digest) string {
	return fmt.Sprintf(blobPathFormat, id.Algorithm(), id.Encoded()[0:2], id.Encoded())
}

func (s *blobStore) pathToDigest(path string) digest.Digest {
	parts := strings.Split(path, string(os.PathSeparator))
	id, err := digest.Parse(fmt.Sprintf("%s:%s", parts[1], parts[3]))
	if err != nil {
		panic("invalid path: " + path)
	}
	return id
}

func (s *blobStore) uuidToPath(id uuid.UUID) string {
	return fmt.Sprintf(uploadPathFormat, id.String())
}

func (s *blobStore) stat(path string) (*store.BlobInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, store.ErrBlobNotFound
	}

	return &store.BlobInfo{
		Size: int64(len(data)),
	}, nil
}

func (s *blobStore) delete(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, path)
	return nil
}

type writer struct {
	s    *blobStore
	path string
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()

	w.s.store[w.path] = append(w.s.store[w.path], p...)
	return len(p), nil
}

func (w *writer) Close() error {
	return nil
}
