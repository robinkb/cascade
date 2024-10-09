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
		GetBlob(digest string) ([]byte, error)
		WriteBlob(digest string, content []byte) error
		StatManifest(name, reference string) (*FileInfo, error)
		GetManifest(name, reference string) ([]byte, error)
		PutManifest(name, reference string, content []byte) error
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
	store        RegistryStore
	sessionStore map[string]map[string]bool
}

func (s *registryService) StatBlob(name, digest string) (*FileInfo, error) {
	id, err := godigest.Parse(digest)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	path := fmt.Sprintf("blobs/%s/%s/%s", id.Algorithm(), id.Encoded()[0:2], id.Encoded())

	info, err := s.store.Stat(path)
	if err != nil {
		switch {
		case errors.Is(err, ErrFileNotFound):
			return nil, ErrBlobUnknown
		default:
			return nil, err
		}
	}

	return info, nil
}

func (s *registryService) GetBlob(digest string) ([]byte, error) {
	id, err := godigest.Parse(digest)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	path := fmt.Sprintf("blobs/%s/%s/%s", id.Algorithm(), id.Encoded()[0:2], id.Encoded())

	data, err := s.store.Get(path)
	if err != nil {
		switch {
		case errors.Is(err, ErrFileNotFound):
			return nil, ErrBlobUnknown
		default:
			return nil, err
		}
	}

	return data, nil
}

func (s *registryService) WriteBlob(digest string, content []byte) error {
	id, err := godigest.Parse(digest)
	if err != nil {
		return ErrDigestInvalid
	}

	path := fmt.Sprintf("blobs/%s/%s/%s", id.Algorithm(), id.Encoded()[0:2], id.Encoded())

	cd := godigest.FromBytes(content)
	if id != cd {
		return ErrDigestInvalid
	}

	s.store.Put(path, content)

	return nil
}

func (s *registryService) StatManifest(name, reference string) (*FileInfo, error) {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	info, err := s.store.Stat(path)
	if err != nil {
		switch {
		case errors.Is(err, ErrFileNotFound):
			return nil, ErrManifestUnknown
		default:
			return nil, err
		}
	}

	return info, nil
}

func (s *registryService) GetManifest(name, reference string) ([]byte, error) {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	content, err := s.store.Get(path)
	if err != nil {
		switch {
		case errors.Is(err, ErrFileNotFound):
			return nil, ErrManifestUnknown
		default:
			return nil, err
		}
	}

	return content, nil
}

func (s *registryService) PutManifest(name, reference string, content []byte) error {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	s.store.Put(path, content)
	return nil
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
