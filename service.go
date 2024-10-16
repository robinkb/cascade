package main

import (
	"encoding"
	"errors"
	"fmt"

	// Required for go-digest.
	"crypto/sha256"
	_ "crypto/sha256"

	"github.com/gofrs/uuid/v5"
	godigest "github.com/opencontainers/go-digest"
)

type (
	RegistryService interface {
		StatBlob(name, digest string) (*FileInfo, error)
		GetBlob(digest string) ([]byte, error)
		StatManifest(name, reference string) (*FileInfo, error)
		GetManifest(name, reference string) ([]byte, error)
		PutManifest(name, reference string, content []byte) error
		InitUpload(name string) *UploadSession
		StatUpload(sessionID string) (*FileInfo, error)
		WriteUpload(sessionID string, content []byte) error
		CloseUpload(id, digest string) error
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
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUnknown
	}

	return info, err
}

func (s *registryService) GetBlob(digest string) ([]byte, error) {
	id, err := godigest.Parse(digest)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	path := fmt.Sprintf("blobs/%s/%s/%s", id.Algorithm(), id.Encoded()[0:2], id.Encoded())

	data, err := s.store.Get(path)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUnknown
	}

	return data, err
}

func (s *registryService) StatUpload(sessionID string) (*FileInfo, error) {
	dataPath := fmt.Sprintf("uploads/%s/data", sessionID)

	info, err := s.store.Stat(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUploadUnknown
	}

	return info, err
}

func (s *registryService) StatManifest(name, reference string) (*FileInfo, error) {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	info, err := s.store.Stat(path)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrManifestUnknown
	}

	return info, err
}

func (s *registryService) GetManifest(name, reference string) ([]byte, error) {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	content, err := s.store.Get(path)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrManifestUnknown
	}

	return content, err
}

func (s *registryService) PutManifest(name, reference string, content []byte) error {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	s.store.Set(path, content)
	return nil
}

func (s *registryService) InitUpload(name string) *UploadSession {
	sessionID, _ := uuid.NewV7()

	hashPath := fmt.Sprintf("uploads/%s/hashstate/sha256", sessionID.String())

	hash := sha256.New()
	_, err := hash.Write([]byte{})
	if err != nil {
		panic(err)
	}

	hashState, err := hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(err)
	}

	err = s.store.Set(hashPath, hashState)
	if err != nil {
		panic(err)
	}

	dataPath := fmt.Sprintf("uploads/%s/data", sessionID)
	err = s.store.Set(dataPath, []byte{})
	if err != nil {
		panic(err)
	}

	return &UploadSession{
		ID:       sessionID.String(),
		Location: fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, sessionID.String()),
	}
}

func (s *registryService) WriteUpload(sessionID string, content []byte) error {
	dataPath := fmt.Sprintf("uploads/%s/data", sessionID)

	_, err := s.StatUpload(sessionID)
	if err != nil {
		return err
	}

	hashPath := fmt.Sprintf("uploads/%s/hashstate/sha256", sessionID)
	hashState, err := s.store.Get(hashPath)
	if err != nil {
		return err
	}

	hash := sha256.New()
	err = hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(hashState)
	if err != nil {
		return err
	}
	_, err = hash.Write(content)
	if err != nil {
		panic(err)
	}

	hashState, err = hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(err)
	}

	err = s.store.Set(hashPath, hashState)
	if err != nil {
		panic(err)
	}

	err = s.store.Put(dataPath, content)
	return err
}

func (s *registryService) CloseUpload(sessionID, digest string) error {
	id, err := godigest.Parse(digest)
	if err != nil {
		return ErrDigestInvalid
	}

	hashPath := fmt.Sprintf("uploads/%s/hashstate/sha256", sessionID)
	hashState, err := s.store.Get(hashPath)
	if err != nil {
		return err
	}

	hash := sha256.New()
	err = hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(hashState)
	if err != nil {
		return err
	}

	calculatedId := godigest.NewDigest(godigest.Canonical, hash)
	if id != calculatedId {
		return ErrDigestInvalid
	}

	sourcePath := fmt.Sprintf("uploads/%s/data", sessionID)
	destPath := fmt.Sprintf("blobs/%s/%s/%s", id.Algorithm(), id.Encoded()[0:2], id.Encoded())

	s.store.Move(sourcePath, destPath)
	return nil
}
