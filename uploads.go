package cascade

import (
	"crypto/sha256"
	"encoding"
	"errors"
	"fmt"
	"io"

	"github.com/robinkb/cascade-registry/paths"

	"github.com/gofrs/uuid/v5"
	godigest "github.com/opencontainers/go-digest"
)

// TODO: Should write a test to verify that uploads can only be accessed
// from the repository where it was created. Spoiler alert: not the case.
func (s *registryService) StatUpload(repository, sessionID string) (*FileInfo, error) {
	path := paths.BlobStore.UploadData(sessionID)

	info, err := s.b.Stat(path)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUploadUnknown
	}

	return info, err
}

// TODO: This should be able to return errors, and verify that upload sessions
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
	w, err := s.b.Writer(dataPath)
	if err != nil {
		panic(err)
	}
	_, err = w.Write([]byte{})
	if err != nil {
		panic(err)
	}

	return &UploadSession{
		ID:       sessionID.String(),
		Location: fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, sessionID.String()),
	}
}

// TODO: Verify that this is properly scoped to a repository.
func (s *registryService) AppendUpload(repository, sessionID string, r io.Reader, offset int64) error {
	info, err := s.StatUpload(repository, sessionID)
	if err != nil {
		return err
	}

	if info.Size != offset {
		return ErrUploadOffsetInvalid
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

	dataPath := paths.BlobStore.UploadData(sessionID)
	w, err := s.b.Writer(dataPath)
	if err != nil {
		return err
	}

	tee := io.TeeReader(r, hash)
	_, err = io.Copy(w, tee)
	if err != nil {
		return err
	}

	hashState, err = hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(err)
	}

	return s.store.Set(hashPath, hashState)
}

func (s *registryService) CloseUpload(repository, sessionID, digest string) error {
	_, err := s.StatUpload(repository, sessionID)
	if err != nil {
		return err
	}

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
		return ErrBlobUploadInvalid
	}

	sourcePath := paths.BlobStore.UploadData(sessionID)
	destPath := paths.BlobStore.BlobData(id)
	linkPath := paths.MetaStore.BlobLink(repository, id)

	s.b.Move(sourcePath, destPath)
	s.store.Set(linkPath, nil)
	return nil
}
