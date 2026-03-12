package suite

import (
	"bytes"
	"slices"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	"github.com/stretchr/testify/suite"
)

type MetadataStoreConstructor func() store.Metadata

type MetadataSuite struct {
	suite.Suite

	Constructor MetadataStoreConstructor
}

func (s *MetadataSuite) TestListBlobs() {
	s.T().Run("lists all blobs across repositories", func(t *testing.T) {
		store := s.Constructor()
		want := make([]digest.Digest, 5)

		for i := range len(want) {
			name := RandomName()
			err := store.CreateRepository(name)
			AssertNoError(t, err).Require()

			want[i] = RandomDigest()
			err = store.PutBlob(name, want[i])
			AssertNoError(t, err).Require()
		}

		slices.Sort(want)

		got, err := store.ListBlobs()
		AssertNoError(t, err)

		slices.Sort(got)
		AssertSlicesEqual(t, got, want)
	})

	s.T().Run("does not return deleted blobs", func(t *testing.T) {
		store := s.Constructor()
		name, digest := RandomName(), RandomDigest()
		err := store.CreateRepository(name)
		AssertNoError(t, err).Require()
		err = store.PutBlob(name, digest)
		AssertNoError(t, err).Require()

		blobs, err := store.ListBlobs()
		AssertNoError(t, err).Require()
		AssertEqual(t, len(blobs), 1).Require()
		AssertEqual(t, blobs[0], digest)

		err = store.DeleteBlob(name, digest)
		AssertNoError(t, err).Require()

		blobs, err = store.ListBlobs()
		AssertNoError(t, err).Require()
		AssertEqual(t, len(blobs), 0)
	})

	s.T().Run("returns a blob deleted in one repository but present in another", func(t *testing.T) {
		store := s.Constructor()
		name1, name2 := RandomName(), RandomName()
		digest := RandomDigest()

		for _, name := range []string{name1, name2} {
			err := store.CreateRepository(name)
			AssertNoError(t, err).Require()
			err = store.PutBlob(name, digest)
			AssertNoError(t, err).Require()
		}

		err := store.DeleteBlob(name1, digest)
		AssertNoError(t, err).Require()

		digests, err := store.ListBlobs()
		AssertNoError(t, err)
		AssertEqual(t, len(digests), 1).Require()
		AssertEqual(t, digests[0], digest)
	})
}

func (s *MetadataSuite) TestSnapshotRestore() {
	s.T().Run("snapshot and restore into another MetadataStore", func(t *testing.T) {
		// The store that we're taking a snapshot from.
		snapshotStore := s.Constructor()
		// The store that we're restoring into.
		restoreStore := s.Constructor()

		name, digest := RandomName(), RandomDigest()

		err := snapshotStore.CreateRepository(name)
		AssertNoError(t, err).Require()

		err = snapshotStore.PutBlob(name, digest)
		AssertNoError(t, err).Require()

		snapshot := new(bytes.Buffer)
		err = snapshotStore.Snapshot(snapshot)
		AssertNoError(t, err).Require()

		err = restoreStore.Restore(snapshot)
		AssertNoError(t, err).Require()

		_, err = restoreStore.GetBlob(name, digest)
		AssertNoError(t, err).Require()
	})

	s.T().Run("snapshot and restore in-place", func(t *testing.T) {
		ms := s.Constructor()
		snapshot := new(bytes.Buffer)

		name, digest := RandomName(), RandomDigest()

		err := ms.CreateRepository(name)
		AssertNoError(t, err).Require()
		err = ms.PutBlob(name, digest)
		AssertNoError(t, err).Require()

		_, err = ms.GetBlob(name, digest)
		AssertNoError(t, err).Require()

		err = ms.Snapshot(snapshot)
		AssertNoError(t, err).Require()

		err = ms.DeleteBlob(name, digest)
		AssertNoError(t, err).Require()

		_, err = ms.GetBlob(name, digest)
		AssertErrorIs(t, err, store.ErrNotFound)

		err = ms.Restore(snapshot)
		AssertNoError(t, err).Require()

		_, err = ms.GetBlob(name, digest)
		AssertNoError(t, err).Require()
	})
}
