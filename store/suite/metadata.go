package suite

import (
	"bytes"
	"testing"

	"github.com/robinkb/cascade-registry/store"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/stretchr/testify/suite"
)

type MetadataStoreConstructor func() store.Metadata

type MetadataSuite struct {
	suite.Suite

	Constructor MetadataStoreConstructor
	store       store.Metadata
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
