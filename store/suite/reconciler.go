package suite

import (
	"bytes"
	"io"
	"slices"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
	. "github.com/robinkb/cascade-registry/testing" // nolint: staticcheck
	"github.com/stretchr/testify/suite"
)

type BlobStoreConstructor func() store.Blobs

type ReconcilerSuite struct {
	suite.Suite

	MetadataStoreConstructor MetadataStoreConstructor
	BlobStoreConstructor     BlobStoreConstructor
}

func (s *ReconcilerSuite) TestReconcile() {
	s.T().Run("reconciles from full source into empty destination", func(t *testing.T) {
		meta := s.MetadataStoreConstructor()
		src := s.BlobStoreConstructor()
		dst := s.BlobStoreConstructor()
		count := 50

		name := RandomName()
		err := meta.CreateRepository(name)
		AssertNoError(t, err).Require()

		for range count {
			id, content := RandomBlob(32)

			err := meta.PutBlob(name, id)
			AssertNoError(t, err).Require()

			w, err := src.BlobWriter(id)
			AssertNoError(t, err).Require()

			_, err = io.Copy(w, bytes.NewBuffer(content))
			AssertNoError(t, err).Require()
		}

		err = store.Reconcile(meta, src, dst)
		AssertNoError(t, err)

		want, err := meta.ListBlobs()
		AssertNoError(t, err)

		got := make([]digest.Digest, 0)
		for id, err := range dst.AllBlobs() {
			AssertNoError(t, err)

			got = append(got, id)
		}

		slices.Sort(got)
		slices.Sort(want)

		AssertEqual(t, len(got), count)
		AssertSlicesEqual(t, got, want)
	})

	// Have to add at least:
	// 1. Destination has blobs that source does not have --> Ensure deleted
	// 2. Source has missing blobs --> Should not happen, but detect error
	//
	// Would be cool if multiple sources can be tried to search for blobs, but that might be a future thing.
}
