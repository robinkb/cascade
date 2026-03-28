package repository

import (
	"testing"

	. "github.com/robinkb/cascade/testing"
)

func TestStatBlob(t *testing.T) {
	blobs, repo, svc := NewTestRepository(t)
	digest, content := RandomBlob(32 * 1024)

	err := blobs.PutBlob(digest, content)
	RequireNoError(t, err)
	err = repo.PutBlob(digest)
	RequireNoError(t, err)

	t.Run("Known blob returns no error", func(t *testing.T) {
		_, err := svc.StatBlob(digest.String())
		AssertNoError(t, err)
	})

	t.Run("Unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := svc.StatBlob(RandomDigest().String())
		AssertErrorIs(t, err, ErrBlobUnknown)
	})

	// TODO: Huh? Why?
	t.Run("Known blob in unknown repository returns ErrBlobUnknown", func(t *testing.T) {
		_, err := svc.StatBlob(digest.String())
		AssertErrorIs(t, err, ErrBlobUnknown)
	})
}

// func (s *Suite) TestGetBlob() {
// 	if s.Tests.BlobsDisabled {
// 		s.T().SkipNow()
// 	}

// 	name := s.RandomRepository()
// 	digest, content := RandomBlob(32)

// 	err := s.blobs.PutBlob(digest, content)
// 	RequireNoError(s.T(), err)
// 	err = s.metadata.PutBlob(name, digest)
// 	RequireNoError(s.T(), err)

// 	s.T().Run("Known blob returns content and no error", func(t *testing.T) {
// 		r, err := s.repository.GetBlob(name, digest.String())
// 		RequireNoError(t, err)
// 		data, err := io.ReadAll(r)
// 		AssertNoError(t, err)
// 		AssertSlicesEqual(t, data, content)
// 	})

// 	s.T().Run("Unknown blob returns no content and ErrBlobUnknown", func(t *testing.T) {
// 		_, err := s.repository.GetBlob("fake/repository", "blabla")
// 		AssertErrorIs(t, err, ErrBlobUnknown)
// 	})

// 	s.T().Run("Known blob on unknown repository still returns no content and ErrBlobUnknown", func(t *testing.T) {
// 		_, err := s.repository.GetBlob("fake/repository", digest.String())
// 		AssertErrorIs(t, err, ErrBlobUnknown)
// 	})
// }

// func (s *Suite) TestDeleteBlob() {
// 	if s.Tests.BlobsDisabled {
// 		s.T().SkipNow()
// 	}

// 	name := s.RandomRepository()
// 	digest, content := RandomBlob(32)

// 	s.T().Run("A deleted blob is not retrievable", func(t *testing.T) {
// 		err := s.blobs.PutBlob(digest, content)
// 		RequireNoError(t, err)
// 		err = s.metadata.PutBlob(name, digest)
// 		RequireNoError(t, err)

// 		_, err = s.repository.GetBlob(name, digest.String())
// 		RequireNoError(t, err)

// 		err = s.repository.DeleteBlob(name, digest.String())
// 		RequireNoError(t, err)

// 		_, err = s.repository.GetBlob(name, digest.String())
// 		AssertErrorIs(t, err, ErrBlobUnknown)
// 	})
// }
