package cascade

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
)

var (
	ErrFileNotFound = errors.New("file not found")
)

type (
	RegistryStore interface {
		Stat(path string) (*FileInfo, error)
		Get(path string) ([]byte, error)
		Set(path string, content []byte) error
		// Reader(path string) (io.Reader, error)
		Put(path string, content []byte) error
		Delete(path string) error
		Move(sourcePath, destinationPath string)
	}

	MetadataStore interface {
		GetBlob(repository string, digest digest.Digest) (string, error)
		PutBlob(repository string, digest digest.Digest, path string) error
		DeleteBlob(repository string, digest digest.Digest) error

		GetManifest(repository string, digest digest.Digest) (string, error)
		PutManifest(repository string, digest digest.Digest, path string) error
		DeleteManifest(repository string, digest digest.Digest) error

		ListTags(repository string) ([]string, error)
		GetTag(repository, tag string) (string, error)
		PutTag(repository, tag, digest string) error
		DeleteTag(repository, tag string) error

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
)

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		store: make(map[string][]byte),
	}
}

type InMemoryStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

func (s *InMemoryStore) Stat(path string) (*FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, ErrFileNotFound
	}

	return &FileInfo{
		Name: path,
		Size: int64(len(data)),
	}, nil
}

func (s *InMemoryStore) Get(path string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, ErrFileNotFound
	}
	return data, nil
}

// TODO: Currently this accepts 'nil' as the content,
// not sure if that is safe behavior.
func (s *InMemoryStore) Set(path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = content
	return nil
}

func (s *InMemoryStore) Put(path string, content []byte) error {
	_, err := s.Stat(path)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = append(s.store[path], content...)

	return nil
}

func (s *InMemoryStore) Delete(path string) error {
	_, err := s.Stat(path)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, path)
	return nil
}

func (s *InMemoryStore) Move(sourcePath, destinationPath string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[destinationPath] = s.store[sourcePath]
	delete(s.store, sourcePath)
}

func NewInMemoryMetadataStore() MetadataStore {
	return &InMemoryMetadataStore{
		store: make(map[string][]byte),
	}
}

type InMemoryMetadataStore struct {
	store map[string][]byte
}

func (s *InMemoryMetadataStore) GetBlob(repository string, digest digest.Digest) (string, error) {
	if path, ok := s.store[s.blobPath(repository, digest)]; ok {
		return string(path), nil
	}
	return "", ErrBlobUnknown
}

func (s *InMemoryMetadataStore) PutBlob(repository string, digest digest.Digest, path string) error {
	s.store[s.blobPath(repository, digest)] = []byte(path)
	return nil
}

func (s *InMemoryMetadataStore) DeleteBlob(repository string, digest digest.Digest) error {
	delete(s.store, s.blobPath(repository, digest))
	return nil
}

func (s *InMemoryMetadataStore) GetManifest(repository string, digest digest.Digest) (string, error) {
	if path, ok := s.store[s.manifestPath(repository, digest)]; ok {
		return string(path), nil
	}
	return "", ErrManifestUnknown
}

func (s *InMemoryMetadataStore) PutManifest(repository string, digest digest.Digest, path string) error {
	s.store[s.manifestPath(repository, digest)] = []byte(path)
	return nil
}

func (s *InMemoryMetadataStore) DeleteManifest(repository string, digest digest.Digest) error {
	delete(s.store, s.manifestPath(repository, digest))
	return nil
}

func (s *InMemoryMetadataStore) ListTags(repository string) ([]string, error) {
	tags := []string{}
	prefix := s.tagPath(repository, "")

	for key := range s.store {
		if tag, found := strings.CutPrefix(key, prefix); found {
			tags = append(tags, tag)
		}
	}

	slices.Sort(tags)

	return tags, nil
}

func (s *InMemoryMetadataStore) GetTag(repository, tag string) (string, error) {
	if digest, ok := s.store[s.tagPath(repository, tag)]; ok {
		return string(digest), nil
	}
	return "", ErrManifestUnknown
}

func (s *InMemoryMetadataStore) PutTag(repository, tag, digest string) error {
	s.store[s.tagPath(repository, tag)] = []byte(digest)
	return nil
}

func (s *InMemoryMetadataStore) DeleteTag(repository, tag string) error {
	delete(s.store, s.tagPath(repository, tag))
	return nil
}

func (s *InMemoryMetadataStore) GetUpload(repository string, id string) (*UploadSession, error) {
	if data, ok := s.store[s.uploadPath(repository, id)]; ok {
		var session UploadSession
		data := bytes.NewBuffer(data)
		err := gob.NewDecoder(data).Decode(&session)
		return &session, err
	}

	return nil, ErrBlobUploadUnknown
}

func (s *InMemoryMetadataStore) PutUpload(repository string, session *UploadSession) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(session)
	if err != nil {
		return err
	}

	s.store[s.uploadPath(repository, session.ID.String())] = buf.Bytes()
	return nil
}

func (s *InMemoryMetadataStore) DeleteUpload(repository string, session string) error {
	delete(s.store, s.uploadPath(repository, session))
	return nil
}

func (s *InMemoryMetadataStore) blobPath(repository string, digest digest.Digest) string {
	return fmt.Sprintf("repositories/%s/blobs/%s/%s", repository, digest.Algorithm(), digest.Encoded())
}

func (s *InMemoryMetadataStore) manifestPath(repository string, digest digest.Digest) string {
	return fmt.Sprintf("repositories/%s/manifests/%s/%s", repository, digest.Algorithm(), digest.Encoded())
}

func (s *InMemoryMetadataStore) tagPath(repository, tag string) string {
	return fmt.Sprintf("repositories/%s/tags/%s", repository, tag)
}

func (s *InMemoryMetadataStore) uploadPath(repository, sessionID string) string {
	return fmt.Sprintf("repositories/%s/uploads/%s", repository, sessionID)
}

func NewInMemoryBlobStore() BlobStore {
	return &InMemoryBlobStore{
		store: make(map[string][]byte),
	}
}

type InMemoryBlobStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

type writer struct {
	s    *InMemoryBlobStore
	path string
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()

	w.s.store[w.path] = append(w.s.store[w.path], p...)
	return len(p), nil
}

func (s *InMemoryBlobStore) Stat(path string) (*FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, ErrFileNotFound
	}

	return &FileInfo{
		Name: path,
		Size: int64(len(data)),
	}, nil
}

func (s *InMemoryBlobStore) Get(path string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.store[path]
	if !ok {
		return nil, ErrFileNotFound
	}
	return data, nil
}

func (s *InMemoryBlobStore) Put(path string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[path] = content

	return nil
}

func (s *InMemoryBlobStore) Reader(path string) (io.Reader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return bytes.NewBuffer(s.store[path]), nil
}

func (s *InMemoryBlobStore) Writer(path string) (io.Writer, error) {
	return &writer{s, path}, nil
}

func (s *InMemoryBlobStore) Delete(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, path)
	return nil
}

func (s *InMemoryBlobStore) Move(sourcePath, destinationPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store[destinationPath] = s.store[sourcePath]
	return nil
}
