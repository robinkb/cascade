package cascade

import (
	"encoding"
	"errors"
	"fmt"

	"crypto/sha256"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	godigest "github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/paths"
)

type (
	RegistryService interface {
		StatBlob(repository, digest string) (*FileInfo, error)
		GetBlob(repository, digest string) ([]byte, error)

		StatManifest(repository, reference string) (*FileInfo, error)
		GetManifest(repository, reference string) ([]byte, error)
		PutManifest(repository, reference string, content []byte) error
		DeleteManifest(repository, reference string) error

		GetTag(repository, tag string) (string, error)
		PutTag(repository, tag, digest string) error
		DeleteTag(repository, tag string) error

		InitUpload(repository string) *UploadSession
		StatUpload(repository, sessionID string) (*FileInfo, error)
		AppendUpload(repository, sessionID string, content []byte) error
		CloseUpload(repository, id, digest string) error
	}

	// TODO: Could refactor to this:
	// RegistryService interface {
	// 	Repository(ctx context.Context, name string) RepositoryService
	// }

	// RepositoryService interface {
	// 	StatBlob(digest string) (*FileInfo, error)
	// 	GetBlob(digest string) ([]byte, error)
	// 	StatManifest(reference string) (*FileInfo, error)
	// 	GetManifest(reference string) ([]byte, error)
	// 	PutManifest(reference string, content []byte) error
	// 	DeleteManifest(reference string) error
	// 	InitUpload() *UploadSession
	// 	StatUpload(sessionID string) (*FileInfo, error)
	// 	WriteUpload(sessionID string, content []byte) error
	// 	CloseUpload(id, digest string) error
	// }

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

func (s *registryService) StatBlob(repository, digest string) (*FileInfo, error) {
	id, err := godigest.Parse(digest)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	linkPath := paths.MetaStore.BlobLink(repository, id)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	dataPath := paths.BlobStore.BlobData(id)
	info, err := s.store.Stat(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUnknown
	}

	return info, err
}

func (s *registryService) GetBlob(repository, digest string) ([]byte, error) {
	id, err := godigest.Parse(digest)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	linkPath := paths.MetaStore.BlobLink(repository, id)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	dataPath := paths.BlobStore.BlobData(id)
	data, err := s.store.Get(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUnknown
	}

	return data, err
}

// TODO: Should write a test to verify that uploads can only be accessed
// from the repository where it was created. Spoiler alert: not the case.
func (s *registryService) StatUpload(repository, sessionID string) (*FileInfo, error) {
	path := paths.BlobStore.UploadData(sessionID)

	info, err := s.store.Stat(path)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUploadUnknown
	}

	return info, err
}

func (s *registryService) StatManifest(repository, id string) (*FileInfo, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrDigestInvalid
	}

	linkPath := paths.MetaStore.ManifestLink(repository, digest)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	dataPath := paths.BlobStore.BlobData(digest)
	info, err := s.store.Stat(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrManifestUnknown
	}

	return info, err
}

func (s *registryService) GetManifest(repository, id string) ([]byte, error) {
	digest, err := digest.Parse(id)
	if err != nil {
		return nil, ErrBlobUnknown
	}

	linkPath := paths.MetaStore.ManifestLink(repository, digest)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return nil, ErrManifestUnknown
	}

	dataPath := paths.BlobStore.BlobData(digest)
	content, err := s.store.Get(dataPath)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrManifestUnknown
	}

	return content, err
}

func (s *registryService) PutManifest(repository, reference string, content []byte) error {
	digest, err := digest.Parse(reference)
	if err != nil {
		return ErrDigestInvalid
	}

	dataPath := paths.BlobStore.BlobData(digest)
	linkPath := paths.MetaStore.ManifestLink(repository, digest)

	s.store.Set(dataPath, content)
	s.store.Set(linkPath, nil)

	return nil
}

func (s *registryService) DeleteManifest(repository, id string) error {
	digest, err := digest.Parse(id)
	if err != nil {
		return ErrBlobUnknown
	}

	linkPath := paths.MetaStore.ManifestLink(repository, digest)
	_, err = s.store.Stat(linkPath)
	if err != nil {
		return ErrManifestUnknown
	}

	dataPath := paths.BlobStore.BlobData(digest)
	s.store.Delete(dataPath)
	s.store.Delete(linkPath)

	return err
}

func (s *registryService) GetTag(repository, tag string) (string, error) {
	if !ValidateTag(tag) {
		return "", ErrTagInvalid
	}

	tagLink := paths.MetaStore.TagLink(repository, tag)
	digest, err := s.store.Get(tagLink)

	return string(digest), err
}

func (s *registryService) PutTag(repository, tag, digest string) error {
	if !ValidateTag(tag) {
		return ErrTagInvalid
	}

	tagLink := paths.MetaStore.TagLink(repository, tag)
	return s.store.Set(tagLink, []byte(digest))
}

func (s *registryService) DeleteTag(repository, tag string) error {
	return errors.New("not implemented")
}

// TODO: This should be able to return errors, and very that upload sessions
// cannot be overwritten _just in case_ the generated UUID is not unique... lol.
func (s *registryService) InitUpload(repository string) *UploadSession {
	sessionID, _ := uuid.NewV7()

	hashPath := paths.MetaStore.UploadHashState(repository, sessionID.String(), "sha256")

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

	dataPath := paths.BlobStore.UploadData(sessionID.String())
	err = s.store.Set(dataPath, []byte{})
	if err != nil {
		panic(err)
	}

	return &UploadSession{
		ID:       sessionID.String(),
		Location: fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, sessionID.String()),
	}
}

// TODO: Verify that this is properly scoped to a repository.
func (s *registryService) AppendUpload(repository, sessionID string, content []byte) error {
	dataPath := paths.BlobStore.UploadData(sessionID)

	_, err := s.StatUpload(repository, sessionID)
	if err != nil {
		return err
	}

	// As of Distribution Spec v1.1, clients and servers do not negotiate
	// the hashing algorithm. So we have to assume sha256 for resumable hashing.
	hashPath := paths.MetaStore.UploadHashState(repository, sessionID, "sha256")
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

func (s *registryService) CloseUpload(repository, sessionID, digest string) error {
	id, err := godigest.Parse(digest)
	if err != nil {
		return ErrDigestInvalid
	}

	hashPath := paths.MetaStore.UploadHashState(repository, sessionID, id.Algorithm())
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

	sourcePath := paths.BlobStore.UploadData(sessionID)
	destPath := paths.BlobStore.BlobData(id)
	linkPath := paths.MetaStore.BlobLink(repository, id)

	s.store.Move(sourcePath, destPath)
	s.store.Set(linkPath, nil)
	return nil
}
