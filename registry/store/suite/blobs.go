package suite

import (
	"bytes"
	"crypto/sha256"
	"io"
	"slices"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	"github.com/stretchr/testify/suite"
)

type BlobStoreConstructor func(t *testing.T) store.Blobs

type BlobSuite struct {
	suite.Suite

	Constructor BlobStoreConstructor
}

func (s *BlobSuite) TestBlobs() {
	s.T().Run("creates and deletes blobs", func(t *testing.T) {
		id, content := RandomBlob(64)
		blobs := s.Constructor(t)

		_, err := blobs.StatBlob(id)
		AssertErrorIs(t, err, store.ErrBlobNotFound)

		err = blobs.PutBlob(id, content)
		AssertNoError(t, err)

		got, err := blobs.GetBlob(id)
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, content)

		err = blobs.DeleteBlob(id)
		AssertNoError(t, err)

		_, err = blobs.StatBlob(id)
		AssertErrorIs(t, err, store.ErrBlobNotFound)
	})

	s.T().Run("returns correct blob info", func(t *testing.T) {
		blobs := s.Constructor(t)
		id, content := RandomBlob(128)

		want := &store.BlobInfo{
			Size: int64(len(content)),
		}

		err := blobs.PutBlob(id, content)
		AssertNoError(t, err)

		got, err := blobs.StatBlob(id)
		AssertNoError(t, err)
		AssertDeepEqual(t, got, want)
	})

	s.T().Run("getting unknown blob returns ErrBlobNotFound", func(t *testing.T) {
		blobs := s.Constructor(t)

		id := RandomDigest()
		_, err := blobs.GetBlob(id)
		AssertErrorIs(t, err, store.ErrBlobNotFound)
	})

	s.T().Run("deleting unknown blob returns ErrBlobNotFound", func(t *testing.T) {
		blobs := s.Constructor(t)

		id := RandomDigest()
		err := blobs.DeleteBlob(id)
		AssertErrorIs(t, err, store.ErrBlobNotFound)
	})

}

func (s *BlobSuite) TestListBlobs() {
	s.T().Run("lists all blobs", func(t *testing.T) {
		count := 10
		blobs := s.Constructor(t)

		want := make([]digest.Digest, 0)
		for range count {
			id, content := RandomBlob(32)
			err := blobs.PutBlob(id, content)
			AssertNoError(t, err).Require()
			want = append(want, id)
		}

		got := make([]digest.Digest, 0)
		for id, err := range blobs.AllBlobs() {
			AssertNoError(t, err)
			got = append(got, id)
		}

		slices.Sort(want)
		slices.Sort(got)

		AssertSlicesEqual(t, got, want)
		AssertEqual(t, len(got), count)
	})
}

func (s *BlobSuite) TestUploads() {
	sessionID, err := uuid.NewV7()
	AssertNoError(s.T(), err).Require()

	s.T().Run("uploads a small blob", func(t *testing.T) {
		blobs := s.Constructor(t)
		id, content := RandomBlob(32 << 10)

		err := blobs.InitUpload(sessionID)
		AssertNoError(s.T(), err)

		w, err := blobs.UploadWriter(sessionID)
		AssertNoError(s.T(), err).Require()

		buf := bytes.NewBuffer(content)
		n, err := io.Copy(w, buf)
		AssertNoError(t, err)
		AssertEqual(t, n, int64(len(content)))

		err = w.Close()
		AssertNoError(t, err)

		err = blobs.CloseUpload(sessionID, id)
		AssertNoError(t, err)

		content, err = blobs.GetBlob(id)
		AssertNoError(t, err)

		got := digest.FromBytes(content)
		AssertEqual(t, got, id)
	})

	s.T().Run("uploads a large blob", func(t *testing.T) {
		id, content := RandomBlob(128 << 20)
		blobs := s.Constructor(t)

		err = blobs.InitUpload(sessionID)
		AssertNoError(t, err)

		w, err := blobs.UploadWriter(sessionID)
		AssertNoError(t, err)

		// TODO: This calls Write() only once with the entire blob.
		// Should find out how to make it call in 32kB chunks for a more effective test.
		buf := bytes.NewBuffer(content)
		n, err := io.Copy(w, buf)
		AssertNoError(t, err)
		AssertEqual(t, n, int64(len(content)))

		err = w.Close()
		AssertNoError(t, err)

		err = blobs.CloseUpload(sessionID, id)
		AssertNoError(t, err)

		r, err := blobs.BlobReader(id)
		AssertNoError(t, err)

		hash := sha256.New()
		n, err = io.Copy(hash, r)
		AssertNoError(t, err)

		got := digest.NewDigest(digest.Canonical, hash)
		AssertEqual(t, got, id)
	})
}
