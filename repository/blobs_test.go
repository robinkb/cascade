package repository_test

import (
	"io"
	"testing"

	"github.com/robinkb/cascade-registry/repository"
	. "github.com/robinkb/cascade-registry/testing"
)

func (s *Suite) TestStatBlob() {
	if s.Tests.BlobsDisabled {
		s.T().SkipNow()
	}

	name := RandomName()
	digest, content := RandomBlob(32 * 1024)

	err := s.blobs.PutBlob(digest, content)
	RequireNoError(s.T(), err)
	err = s.metadata.PutBlob(name, digest)
	RequireNoError(s.T(), err)

	s.T().Run("Known blob returns no error", func(t *testing.T) {
		_, err := s.repository.StatBlob(name, digest.String())
		AssertNoError(t, err)
	})

	s.T().Run("Unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := s.repository.StatBlob("a", "b")
		AssertErrorIs(t, err, repository.ErrBlobUnknown)
	})

	s.T().Run("Known blob in unknown repository returns ErrBlobUnknown", func(t *testing.T) {
		_, err := s.repository.StatBlob("fake/repository", digest.String())
		AssertErrorIs(t, err, repository.ErrBlobUnknown)
	})
}

func (s *Suite) TestGetBlob() {
	if s.Tests.BlobsDisabled {
		s.T().SkipNow()
	}

	name := RandomName()
	digest, content := RandomBlob(32)

	err := s.blobs.PutBlob(digest, content)
	RequireNoError(s.T(), err)
	err = s.metadata.PutBlob(name, digest)
	RequireNoError(s.T(), err)

	s.T().Run("Known blob returns content and no error", func(t *testing.T) {
		r, err := s.repository.GetBlob(name, digest.String())
		AssertNoError(t, err)
		data, err := io.ReadAll(r)
		AssertNoError(t, err)
		AssertSlicesEqual(t, data, content)
	})

	s.T().Run("Unknown blob returns no content and ErrBlobUnknown", func(t *testing.T) {
		_, err := s.repository.GetBlob("fake/repository", "blabla")
		AssertErrorIs(t, err, repository.ErrBlobUnknown)
	})

	s.T().Run("Known blob on unknown repository still returns no content and ErrBlobUnknown", func(t *testing.T) {
		_, err := s.repository.GetBlob("fake/repository", digest.String())
		AssertErrorIs(t, err, repository.ErrBlobUnknown)
	})
}

func (s *Suite) TestDeleteBlob() {
	if s.Tests.BlobsDisabled {
		s.T().SkipNow()
	}

	name := RandomName()
	digest, content := RandomBlob(32)

	s.T().Run("A deleted blob is not retrievable", func(t *testing.T) {
		err := s.blobs.PutBlob(digest, content)
		RequireNoError(t, err)
		err = s.metadata.PutBlob(name, digest)
		RequireNoError(t, err)

		_, err = s.repository.GetBlob(name, digest.String())
		RequireNoError(t, err)

		err = s.repository.DeleteBlob(name, digest.String())
		RequireNoError(t, err)

		_, err = s.repository.GetBlob(name, digest.String())
		AssertErrorIs(t, err, repository.ErrBlobUnknown)
	})
}
