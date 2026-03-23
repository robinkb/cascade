package suite

import (
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

	// Toggles to skip sets of tests.
	// When implementing a new Metadata driver,
	// it is recommended to pass the tests from top to bottom.

	SkipRepository    bool // repository management
	SkipBlob          bool // blob management
	SkipListBlobs     bool // listing blobs across repositories
	SkipManifest      bool // manifest management
	SkipListManifests bool // listing blobs across repositories, including manifests
}

func (s *MetadataSuite) RepositoryConstructor(t *testing.T) store.Repository {
	meta := s.Constructor()
	name := RandomName()
	repo, err := meta.CreateRepository(name)
	AssertNoError(t, err).Require()

	t.Cleanup(func() {
		err := meta.DeleteRepository(name)
		AssertNoError(t, err).Require()
	})

	return repo
}

func (s *MetadataSuite) TestRepository() {
	if s.SkipRepository {
		s.T().Skip()
	}

	s.T().Run("creates a new repository", func(t *testing.T) {
		meta := s.Constructor()
		name := RandomName()

		_, err := meta.GetRepository(name)
		AssertErrorIs(t, err, store.ErrRepositoryNotFound)

		_, err = meta.CreateRepository(name)
		AssertNoError(t, err)

		_, err = meta.GetRepository(name)
		AssertNoError(t, err)
	})

	s.T().Run("deletes an existing repository", func(t *testing.T) {
		meta := s.Constructor()
		name := RandomName()

		_, err := meta.CreateRepository(name)
		AssertNoError(t, err)

		_, err = meta.GetRepository(name)
		AssertNoError(t, err)

		err = meta.DeleteRepository(name)
		AssertNoError(t, err)

		_, err = meta.GetRepository(name)
		AssertErrorIs(t, err, store.ErrRepositoryNotFound)
	})

	s.T().Run("creating repository with the same name returns ErrRepositoryExists", func(t *testing.T) {
		meta := s.Constructor()
		name := RandomName()

		_, err := meta.CreateRepository(name)
		AssertNoError(t, err)

		_, err = meta.CreateRepository(name)
		AssertErrorIs(t, err, store.ErrRepositoryExists)
	})

	s.T().Run("deleting unknown repository returns ErrRepositoryNotFound", func(t *testing.T) {
		meta := s.Constructor()
		name := RandomName()

		err := meta.DeleteRepository(name)
		AssertErrorIs(t, err, store.ErrRepositoryNotFound)
	})
}

func (s *MetadataSuite) TestBlobs() {
	if s.SkipBlob {
		s.T().Skip()
	}

	s.T().Run("creates a new blob", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.GetBlob(digest)
		AssertErrorIs(t, err, store.ErrRepositoryBlobNotFound)

		err = repo.PutBlob(digest)
		AssertNoError(t, err)

		err = repo.GetBlob(digest)
		AssertNoError(t, err)
	})

	s.T().Run("deletes an existing blob", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.PutBlob(digest)
		AssertNoError(t, err)

		err = repo.GetBlob(digest)
		AssertNoError(t, err)

		err = repo.DeleteBlob(digest)
		AssertNoError(t, err)

		err = repo.GetBlob(digest)
		AssertErrorIs(t, err, store.ErrRepositoryBlobNotFound)
	})

	s.T().Run("deleting unknown blob returns ErrRepositoryBlobNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.DeleteBlob(digest)
		AssertErrorIs(t, err, store.ErrRepositoryBlobNotFound)
	})
}

func (s *MetadataSuite) TestListBlobs() {
	if s.SkipListBlobs {
		s.T().Skip()
	}

	s.T().Run("lists blobs across repositories", func(t *testing.T) {
		meta := s.Constructor()
		want := make([]digest.Digest, 5)
		got := make([]digest.Digest, 0)

		for i := range len(want) {
			name := RandomName()
			repo, err := meta.CreateRepository(name)
			AssertNoError(t, err).Require()

			want[i] = RandomDigest()
			err = repo.PutBlob(want[i])
			AssertNoError(t, err).Require()
		}

		for digest := range meta.Blobs() {
			got = append(got, digest)
		}

		slices.Sort(want)
		slices.Sort(got)

		AssertSlicesEqual(t, got, want)
	})

	s.T().Run("does not return deleted blobs", func(t *testing.T) {
		meta := s.Constructor()
		name, digest := RandomName(), RandomDigest()
		repo, err := meta.CreateRepository(name)
		AssertNoError(t, err).Require()

		err = repo.PutBlob(digest)
		AssertNoError(t, err).Require()

		blobs := slices.Collect(meta.Blobs())
		AssertEqual(t, len(blobs), 1).Require()
		AssertEqual(t, blobs[0], digest)

		err = repo.DeleteBlob(digest)
		AssertNoError(t, err).Require()

		blobs = slices.Collect(meta.Blobs())
		AssertNoError(t, err).Require()
		AssertEqual(t, len(blobs), 0)
	})

	s.T().Run("returns a blob deleted in one repository but present in another", func(t *testing.T) {
		meta := s.Constructor()
		count := 5
		repos := make([]store.Repository, count)
		digest := RandomDigest()

		var err error
		for i := range repos {
			repos[i], err = meta.CreateRepository(RandomName())
			AssertNoError(t, err).Require()
			err = repos[i].PutBlob(digest)
			AssertNoError(t, err).Require()
		}

		err = repos[0].DeleteBlob(digest)
		AssertNoError(t, err).Require()

		digests := slices.Collect(meta.Blobs())
		AssertEqual(t, len(digests), 1).Require()
		AssertEqual(t, digests[0], digest)
	})
}

func (s *MetadataSuite) TestManifests() {
	if s.SkipManifest {
		s.T().Skip()
	}

	s.T().Run("creates a new manifest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest, want := RandomDigest(), RandomManifestMetadata()

		_, err := repo.GetManifest(digest)
		AssertErrorIs(t, err, store.ErrManifestNotFound)

		err = repo.PutManifest(digest, want, store.References{})
		AssertNoError(t, err)

		got, err := repo.GetManifest(digest)
		AssertNoError(t, err)
		AssertDeepEqual(t, got, want)
	})

	s.T().Run("creates a blob for the manifest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.GetBlob(digest)
		AssertErrorIs(t, err, store.ErrRepositoryBlobNotFound)

		err = repo.PutManifest(digest, store.Manifest{}, store.References{})
		AssertNoError(t, err)

		err = repo.GetBlob(digest)
		AssertNoError(t, err)
	})

	s.T().Run("deletes an existing manifest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{})
		AssertNoError(t, err)

		_, err = repo.GetManifest(digest)
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(digest)
		AssertNoError(t, err)
		AssertEqual(t, len(deleted), 1).Require()
		AssertEqual(t, deleted[0], digest)

		_, err = repo.GetManifest(digest)
		AssertErrorIs(t, err, store.ErrManifestNotFound)
	})

	s.T().Run("deletes the manifest's associated blob", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{})
		AssertNoError(t, err)

		err = repo.GetBlob(digest)
		AssertNoError(t, err)

		_, err = repo.DeleteManifest(digest)
		AssertNoError(t, err)

		err = repo.GetBlob(digest)
		AssertErrorIs(t, err, store.ErrRepositoryBlobNotFound)
	})

	s.T().Run("deleting unknown manifest returns ErrManifestNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		_, err := repo.DeleteManifest(digest)
		AssertErrorIs(t, err, store.ErrManifestNotFound)
	})

	s.T().Run("deletes a referenced config manifest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		configDigest, manifestDigest := RandomDigest(), RandomDigest()
		digests := []digest.Digest{
			configDigest,
			manifestDigest,
		}

		err := repo.PutBlob(configDigest)
		AssertNoError(t, err)

		err = repo.PutManifest(manifestDigest, store.Manifest{}, store.References{
			Config: configDigest,
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(manifestDigest)
		AssertNoError(t, err)
		slices.Sort(deleted)
		slices.Sort(digests)
		AssertSlicesEqual(t, deleted, digests)

		err = repo.GetBlob(configDigest)
		AssertErrorIs(t, err, store.ErrRepositoryBlobNotFound)
	})

	s.T().Run("creating manifest referencing unknown config blob returns ErrManifestConfigNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{
			Config: RandomDigest(),
		})
		AssertErrorIs(t, err, store.ErrManifestInvalid, store.ErrManifestConfigNotFound)
	})
}

// func (s *MetadataSuite) TestSnapshotRestore() {
// 	s.T().Run("snapshot and restore into another MetadataStore", func(t *testing.T) {
// 		// The store that we're taking a snapshot from.
// 		snapshotStore := s.Constructor()
// 		// The store that we're restoring into.
// 		restoreStore := s.Constructor()

// 		name, digest := RandomName(), RandomDigest()

// 		err := snapshotStore.CreateRepository(name)
// 		AssertNoError(t, err).Require()

// 		err = snapshotStore.PutBlob(name, digest)
// 		AssertNoError(t, err).Require()

// 		snapshot := new(bytes.Buffer)
// 		err = snapshotStore.Snapshot(snapshot)
// 		AssertNoError(t, err).Require()

// 		err = restoreStore.Restore(snapshot)
// 		AssertNoError(t, err).Require()

// 		_, err = restoreStore.GetBlob(name, digest)
// 		AssertNoError(t, err).Require()
// 	})

// 	s.T().Run("snapshot and restore in-place", func(t *testing.T) {
// 		ms := s.Constructor()
// 		snapshot := new(bytes.Buffer)

// 		name, digest := RandomName(), RandomDigest()

// 		err := ms.CreateRepository(name)
// 		AssertNoError(t, err).Require()
// 		err = ms.PutBlob(name, digest)
// 		AssertNoError(t, err).Require()

// 		_, err = ms.GetBlob(name, digest)
// 		AssertNoError(t, err).Require()

// 		err = ms.Snapshot(snapshot)
// 		AssertNoError(t, err).Require()

// 		err = ms.DeleteBlob(name, digest)
// 		AssertNoError(t, err).Require()

// 		_, err = ms.GetBlob(name, digest)
// 		AssertErrorIs(t, err, store.ErrNotFound)

// 		err = ms.Restore(snapshot)
// 		AssertNoError(t, err).Require()

// 		_, err = ms.GetBlob(name, digest)
// 		AssertNoError(t, err).Require()
// 	})
// }
