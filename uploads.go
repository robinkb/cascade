package cascade

import (
	"crypto/sha256"
	"encoding"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gofrs/uuid/v5"
	godigest "github.com/opencontainers/go-digest"
)

// TODO: Should write a test to verify that uploads can only be accessed
// from the repository where it was created. Spoiler alert: not the case.
func (s *registryService) StatUpload(repository, sessionID string) (*FileInfo, error) {
	session, err := s.metadata.GetUpload(repository, sessionID)
	if err != nil {
		return nil, err
	}

	path := fmt.Sprintf("uploads/%s", session.ID)

	info, err := s.blobs.Stat(path)
	if errors.Is(err, ErrFileNotFound) {
		return nil, ErrBlobUploadUnknown
	}

	return info, err
}

// TODO: This should be able to return errors, and verify that upload sessions
// cannot be overwritten _just in case_ the generated UUID is not unique... lol.
func (s *registryService) InitUpload(repository string) *UploadSession {
	id, _ := uuid.NewV7()

	hash := sha256.New()
	_, err := hash.Write([]byte{})
	if err != nil {
		panic(err)
	}

	hashState, err := hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(err)
	}

	path := fmt.Sprintf("uploads/%s", id.String())

	session := UploadSession{
		ID: id,
		// TODO: The location URL really shouldn't be included here.
		// That's an HTTP implementation detail.
		Location:  fmt.Sprintf("/v2/%s/blobs/uploads/%s", repository, id.String()),
		StartDate: time.Now(),
		HashState: hashState,
		BlobPath:  path,
	}

	err = s.blobs.Put(path, []byte{})
	if err != nil {
		panic(err)
	}

	err = s.metadata.PutUpload(repository, &session)
	if err != nil {
		panic(err)
	}

	return &session
}

func (s *registryService) AppendUpload(repository, sessionID string, r io.Reader, offset int64) error {
	session, err := s.metadata.GetUpload(repository, sessionID)
	if err != nil {
		return err
	}

	info, err := s.blobs.Stat(session.BlobPath)
	if err != nil {
		return err
	}

	if info.Size != offset {
		return ErrUploadOffsetInvalid
	}

	hash := sha256.New()
	err = hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(session.HashState)
	if err != nil {
		return err
	}

	w, err := s.blobs.Writer(session.BlobPath)
	if err != nil {
		return err
	}

	tee := io.TeeReader(r, hash)
	_, err = io.Copy(w, tee)
	if err != nil {
		return err
	}

	session.HashState, err = hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		panic(err)
	}

	return s.metadata.PutUpload(repository, session)
}

func (s *registryService) CloseUpload(repository, sessionID, digest string) error {
	session, err := s.metadata.GetUpload(repository, sessionID)
	if err != nil {
		return err
	}

	id, err := godigest.Parse(digest)
	if err != nil {
		return ErrDigestInvalid
	}

	hash := sha256.New()
	err = hash.(encoding.BinaryUnmarshaler).UnmarshalBinary(session.HashState)
	if err != nil {
		return err
	}

	calculatedId := godigest.NewDigest(godigest.Canonical, hash)
	if id != calculatedId {
		return ErrBlobUploadInvalid
	}

	destPath := fmt.Sprintf("blobs/%s/%s/%s", calculatedId.Algorithm(), calculatedId.Encoded()[0:2], calculatedId.Encoded())

	s.blobs.Move(session.BlobPath, destPath)

	return s.metadata.PutBlob(repository, calculatedId, destPath)
}
