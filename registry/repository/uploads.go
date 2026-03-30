package repository

import (
	"crypto/sha256"
	"encoding"
	"errors"
	"io"
	"time"

	"github.com/gofrs/uuid/v5"
	godigest "github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
)

func (s *repositoryService) StatUpload(sessionID string) (*store.BlobInfo, error) {
	id, err := uuid.FromString(sessionID)
	if err != nil {
		return nil, err
	}

	session, err := s.repo.GetUploadSession(id)
	if err != nil {
		if errors.Is(err, store.ErrUploadNotFound) {
			err = ErrBlobUploadUnknown
		}
		return nil, err
	}

	return s.blobs.StatUpload(session.ID)
}

// TODO: This should verify that upload sessions cannot be overwritten _just in case_ the generated UUID is not unique... lol.
func (s *repositoryService) InitUpload() (*store.UploadSession, error) {
	id, _ := uuid.NewV7()

	hash := sha256.New()
	_, err := hash.Write([]byte{})
	if err != nil {
		return nil, err
	}

	hashState, err := hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return nil, err
	}

	session := store.UploadSession{
		ID:        id,
		StartDate: time.Now(),
		HashState: hashState,
	}

	err = s.blobs.InitUpload(id)
	if err != nil {
		return nil, err
	}

	err = s.repo.PutUploadSession(&session)
	if err != nil {
		if errors.Is(err, store.ErrRepositoryNotFound) {
			err = ErrNameUnknown
		}
		return nil, err
	}

	return &session, nil
}

func (s *repositoryService) AppendUpload(sessionID string, r io.Reader, offset int64) error {
	id, err := uuid.FromString(sessionID)
	if err != nil {
		return err
	}

	session, err := s.repo.GetUploadSession(id)
	if err != nil {
		if errors.Is(err, store.ErrUploadNotFound) {
			err = ErrBlobUploadUnknown
		}
		return err
	}

	info, err := s.blobs.StatUpload(session.ID)
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

	w, err := s.blobs.UploadWriter(session.ID)
	if err != nil {
		return err
	}

	tee := io.TeeReader(r, hash)
	_, err = io.Copy(w, tee)
	if err != nil {
		return err
	}

	err = w.Close()
	if err != nil {
		return err
	}

	session.HashState, err = hash.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return err
	}

	return s.repo.PutUploadSession(session)
}

func (s *repositoryService) CloseUpload(sessionID, digest string) error {
	sid, err := uuid.FromString(sessionID)
	if err != nil {
		return err
	}

	session, err := s.repo.GetUploadSession(sid)
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

	err = s.blobs.CloseUpload(session.ID, calculatedId)
	if err != nil {
		return err
	}

	err = s.repo.PutBlob(calculatedId)
	if err != nil {
		return err
	}

	return s.repo.DeleteUploadSession(sid)
}
