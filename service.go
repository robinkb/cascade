package main

import (
	"errors"
	"fmt"

	// Required for go-digest.
	_ "crypto/sha256"

	"github.com/gofrs/uuid/v5"
	godigest "github.com/opencontainers/go-digest"
)

type (
	RegistryService interface {
		StatBlob(name, digest string) (*FileInfo, error)
		GetBlob(name, digest string) []byte
		WriteBlob(name string, digest string, content []byte) error
		StatManifest(name, reference string) (bool, int)
		GetManifest(name, reference string) []byte
		PutManifest(name, reference string, data []byte)
		InitUploadSession(name string) *UploadSession
		ActiveUploadSession(name, id string) bool
	}

	UploadSession struct {
		ID, Location string
	}
)

func NewRegistryService(store RegistryStore) *registryService {
	return &registryService{
		store:        store,
		sessionStore: make(map[string]map[string]bool),
	}
}

type registryService struct {
	store         RegistryStore
	blobStore     map[string]map[string][]byte
	manifestStore map[string]map[string][]byte
	sessionStore  map[string]map[string]bool
}

// TODO: Blobs should be stored in a Merkle tree.
func (s *registryService) StatBlob(name, digest string) (*FileInfo, error) {
	path := fmt.Sprintf("blobs/%s/%s", name, digest)
	return s.store.Stat(path)
}

// TODO: Blobs should be stored in a Merkle tree.
func (s *registryService) GetBlob(name, digest string) []byte {
	path := fmt.Sprintf("blobs/%s/%s", name, digest)
	content, _ := s.store.Get(path)
	return content
}

// TODO: Blobs should be stored in a Merkle tree.
func (s *registryService) WriteBlob(name string, digest string, content []byte) error {
	d, err := godigest.Parse(digest)
	if err != nil {
		return err
	}

	path := fmt.Sprintf("blobs/%s/%s", name, d.String())

	cd := godigest.FromBytes(content)
	if d != cd {
		return errors.New("TODO: proper error")
	}

	s.store.Put(path, content)

	return nil
}

func (s *registryService) StatManifest(name, reference string) (bool, int) {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	info, err := s.store.Stat(path)
	if err != nil {
		return false, 0
	}

	return true, int(info.Size)
}

func (s *registryService) GetManifest(name, reference string) []byte {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	content, err := s.store.Get(path)
	if err != nil {
		return nil
	}

	return content
}

func (s *registryService) PutManifest(name, reference string, content []byte) {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	s.store.Put(path, content)
}

func (s *registryService) InitUploadSession(name string) *UploadSession {
	id, _ := uuid.NewV7()
	if _, ok := s.sessionStore[name]; !ok {
		s.sessionStore[name] = make(map[string]bool)
	}
	sid := id.String()
	s.sessionStore[name][sid] = true
	return &UploadSession{
		ID:       sid,
		Location: fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, sid),
	}
}

func (s *registryService) ActiveUploadSession(name, id string) bool {
	if _, ok := s.sessionStore[name]; ok {
		return s.sessionStore[name][id]
	}
	return false
}
