package suite

import (
	"bytes"
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
	s.T().Run("uploads a large blob", func(t *testing.T) {
		id, content := RandomBlob(128 << 20)
		blobs := s.Constructor(t)

		sessionID, err := uuid.NewV7()
		AssertNoError(t, err)

		err = blobs.InitUpload(sessionID)
		AssertNoError(t, err)

		w, err := blobs.UploadWriter(sessionID)
		AssertNoError(t, err)

		// TODO: This calls Write() only once with the entire blob.
		// Should find out how to make it call in 32kB chunks for a more effective test.
		r := bytes.NewBuffer(content)
		n, err := io.Copy(w, r)
		AssertNoError(t, err)
		AssertEqual(t, n, int64(len(content)))

		err = blobs.CloseUpload(sessionID, id)
		AssertNoError(t, err)
	})
}
