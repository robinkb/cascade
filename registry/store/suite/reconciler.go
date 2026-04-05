package suite

import (
	"slices"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	"github.com/stretchr/testify/suite"
)

type ReconcilerSuite struct {
	suite.Suite

	MetadataStoreConstructor MetadataStoreConstructor
	BlobStoreConstructor     BlobStoreConstructor
}

func (s *ReconcilerSuite) TestReconcile() {
	s.T().Run("reconciles from full source into empty destination", func(t *testing.T) {
		meta := s.MetadataStoreConstructor(t)
		blobs := s.BlobStoreConstructor(t)
		src := s.BlobStoreConstructor(t)
		count := 10

		name := RandomName()
		repo, err := meta.CreateRepository(name)
		AssertNoError(t, err).Require()

		want := make([]digest.Digest, 0)
		for range count {
			id, content := RandomBlob(32)
			want = append(want, id)

			err := repo.PutBlob(id)
			AssertNoError(t, err).Require()

			err = src.PutBlob(id, content)
			AssertNoError(t, err).Require()
		}

		err = store.Reconcile(meta, blobs, src)
		AssertNoError(t, err)

		got := make([]digest.Digest, 0)
		for id, err := range blobs.AllBlobs() {
			AssertNoError(t, err)
			got = append(got, id)
		}

		slices.Sort(got)
		slices.Sort(want)

		AssertEqual(t, len(got), count)
		AssertSlicesEqual(t, got, want)
	})
}
