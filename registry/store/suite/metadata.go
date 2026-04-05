package suite

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	"github.com/stretchr/testify/suite"
)

type MetadataStoreConstructor func(t *testing.T) store.Metadata

type MetadataSuite struct {
	suite.Suite

	Constructor MetadataStoreConstructor

	// Toggles to skip sets of tests.
	// When implementing a new Metadata driver,
	// it is recommended to pass the tests from top to bottom.

	SkipRepository       bool // repository management
	SkipListRepositories bool // listing repositories
	SkipBlob             bool // blob management
	SkipListBlobs        bool // listing blobs across repositories
	SkipManifest         bool // manifest management
	SkipListManifests    bool // listing blobs across repositories, including manifest blobs
	SkipReferrers        bool // tracking and retrieving referrers
	SkipTags             bool // tag management and listing
	SkipListTags         bool // listing tags
	SkipUploadSessions   bool // upload session management
	SkipRecursiveGC      bool // garbage collection of complex objects
	SkipSnapshotRestore  bool // snapshotting and restoring
}

func (s *MetadataSuite) RepositoryConstructor(t *testing.T) store.Repository {
	meta := s.Constructor(t)
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
		meta := s.Constructor(t)
		name := RandomName()

		_, err := meta.GetRepository(name)
		AssertErrorIs(t, err, store.ErrRepositoryNotFound)

		_, err = meta.CreateRepository(name)
		AssertNoError(t, err)

		_, err = meta.GetRepository(name)
		AssertNoError(t, err)
	})

	s.T().Run("deletes an existing repository", func(t *testing.T) {
		meta := s.Constructor(t)
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
		meta := s.Constructor(t)
		name := RandomName()

		_, err := meta.CreateRepository(name)
		AssertNoError(t, err)

		_, err = meta.CreateRepository(name)
		AssertErrorIs(t, err, store.ErrRepositoryExists)
	})

	s.T().Run("deleting unknown repository returns ErrRepositoryNotFound", func(t *testing.T) {
		meta := s.Constructor(t)
		name := RandomName()

		err := meta.DeleteRepository(name)
		AssertErrorIs(t, err, store.ErrRepositoryNotFound)
	})
}

func (s *MetadataSuite) TestListRepositories() {
	if s.SkipListRepositories {
		s.T().Skip()
	}

	names := []string{
		"adoring/ardinghelli", "hardcore/heyrovsky", "confident/pike", "zen/booth", "flamboyant/fermi",
		"sad/payne", "suspicious/margulis", "angry/bassi", "goofy/archimedes", "inspiring/solomon",
	}
	sortedNames := []string{
		"adoring/ardinghelli", "angry/bassi", "confident/pike", "flamboyant/fermi", "goofy/archimedes",
		"hardcore/heyrovsky", "inspiring/solomon", "sad/payne", "suspicious/margulis", "zen/booth",
	}

	tc := []struct {
		name  string
		count int
		last  string
		want  []string
		err   error
	}{
		{
			name:  "returns all names in lexical order",
			count: -1,
			last:  "",
			want:  sortedNames,
		},
		{
			name:  "returns number of names equal to count param",
			count: 5,
			last:  "",
			want:  []string{"adoring/ardinghelli", "angry/bassi", "confident/pike", "flamboyant/fermi", "goofy/archimedes"},
		},
		{
			name:  "returns empty list when count param is 0",
			count: 0,
			last:  "",
			want:  []string{},
		},
		{
			name:  "returns all names when count param is greater than number of names",
			count: 20,
			last:  "",
			want:  sortedNames,
		},
		{
			name:  "returns ErrRepositoryNotFound when last is not found",
			count: -1,
			last:  "intelligent/musk",
			err:   store.ErrRepositoryNotFound,
		},
		{
			name:  "only returns names after name given as last",
			count: 2,
			last:  "sad/payne",
			want:  []string{"suspicious/margulis", "zen/booth"},
		},
		{
			name:  "count may be set to -1 to return all names after last name",
			count: -1,
			last:  "goofy/archimedes",
			want:  []string{"hardcore/heyrovsky", "inspiring/solomon", "sad/payne", "suspicious/margulis", "zen/booth"},
		},
		{
			name:  "count in combination with last does not go out of bounds",
			count: 3,
			last:  "sad/payne",
			want:  []string{"suspicious/margulis", "zen/booth"},
		},
	}

	for _, tt := range tc {
		s.T().Run(tt.name, func(t *testing.T) {
			meta := s.Constructor(t)

			for _, name := range names {
				_, err := meta.CreateRepository(name)
				AssertNoError(t, err).Require()
			}

			got, err := meta.ListRepositories(tt.count, tt.last)
			t.Logf("%#v", got)
			AssertErrorIs(t, err, tt.err)
			AssertSlicesEqual(t, got, tt.want)
		})
	}
}

func (s *MetadataSuite) TestBlobs() {
	if s.SkipBlob {
		s.T().Skip()
	}

	s.T().Run("creates a new blob", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.GetBlob(digest)
		AssertErrorIs(t, err, store.ErrBlobNotFound)

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
		AssertErrorIs(t, err, store.ErrBlobNotFound)
	})

	s.T().Run("deleting unknown blob returns ErrBlobNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.DeleteBlob(digest)
		AssertErrorIs(t, err, store.ErrBlobNotFound)
	})
}

func (s *MetadataSuite) TestListBlobs() {
	if s.SkipListBlobs {
		s.T().Skip()
	}

	s.T().Run("lists blobs across repositories", func(t *testing.T) {
		meta := s.Constructor(t)
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
		meta := s.Constructor(t)
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
		meta := s.Constructor(t)
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

		for i := range count / 2 {
			err = repos[i].DeleteBlob(digest)
			AssertNoError(t, err).Require()
		}

		digests := slices.Collect(meta.Blobs())
		AssertEqual(t, len(digests), 1).Require()
		AssertEqual(t, digests[0], digest)
	})

	s.T().Run("does not return blobs from deleted repositories", func(t *testing.T) {
		meta := s.Constructor(t)
		name := RandomName()
		repo, err := meta.CreateRepository(name)
		AssertNoError(t, err)

		err = repo.PutBlob(RandomDigest())
		AssertNoError(t, err)

		blobs := slices.Collect(meta.Blobs())
		AssertEqual(t, len(blobs), 1)

		err = meta.DeleteRepository(name)
		AssertNoError(t, err)

		blobs = slices.Collect(meta.Blobs())
		AssertEqual(t, len(blobs), 0)
	})

	s.T().Run("returns blobs deleted in one repository and present in another", func(t *testing.T) {
		meta := s.Constructor(t)
		digest := RandomDigest()
		nameA, nameB := RandomName(), RandomName()
		repoA, err := meta.CreateRepository(nameA)
		AssertNoError(t, err)
		repoB, err := meta.CreateRepository(nameB)
		AssertNoError(t, err)

		err = repoA.PutBlob(digest)
		AssertNoError(t, err)
		err = repoB.PutBlob(digest)
		AssertNoError(t, err)

		blobs := slices.Collect(meta.Blobs())
		AssertEqual(t, len(blobs), 1)

		err = meta.DeleteRepository(nameB)
		AssertNoError(t, err)

		blobs = slices.Collect(meta.Blobs())
		AssertEqual(t, len(blobs), 1)
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
		AssertErrorIs(t, err, store.ErrBlobNotFound)

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
		AssertErrorIs(t, err, store.ErrBlobNotFound)
	})

	s.T().Run("manifest owns its own blob", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		id := RandomDigest()

		err := repo.PutManifest(id, store.Manifest{}, store.References{})
		AssertNoError(t, err)

		err = repo.DeleteBlob(id)
		AssertErrorIs(t, err, store.ErrBlobInUse)
	})

	s.T().Run("deleting unknown manifest returns ErrManifestNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		_, err := repo.DeleteManifest(digest)
		AssertErrorIs(t, err, store.ErrManifestNotFound)
	})

	s.T().Run("update an existing manifest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		id := RandomDigest()

		err := repo.PutManifest(id, store.Manifest{}, store.References{})
		AssertNoError(t, err)
		err = repo.PutManifest(id, store.Manifest{}, store.References{})
		AssertNoError(t, err)
	})

	s.T().Run("deletes a referenced manifest config blob", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		configDigest, manifestDigest := RandomDigest(), RandomDigest()
		digests := []digest.Digest{
			configDigest, manifestDigest,
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
		AssertErrorIs(t, err, store.ErrBlobNotFound)
	})

	// This case does not happen under normal circumstances; a config blob is unique to each manifest.
	s.T().Run("does not delete a config blob referenced by multiple manifests", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		manifestDigestA, manifestDigestB := RandomDigest(), RandomDigest()
		configDigest := RandomDigest()
		digests := []digest.Digest{manifestDigestB, configDigest}
		slices.Sort(digests)

		err := repo.PutBlob(configDigest)
		AssertNoError(t, err)

		err = repo.PutManifest(manifestDigestA, store.Manifest{}, store.References{
			Config: configDigest,
		})
		AssertNoError(t, err)
		err = repo.PutManifest(manifestDigestB, store.Manifest{}, store.References{
			Config: configDigest,
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(manifestDigestA)
		AssertNoError(t, err)
		AssertEqual(t, len(deleted), 1).Require()
		AssertEqual(t, deleted[0], manifestDigestA)

		deleted, err = repo.DeleteManifest(manifestDigestB)
		AssertNoError(t, err)
		slices.Sort(deleted)
		AssertSlicesEqual(t, deleted, digests)
	})

	s.T().Run("creating manifest referencing unknown config blob returns ErrManifestConfigNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{
			Config: RandomDigest(),
		})
		AssertErrorIs(t, err, store.ErrManifestInvalid, store.ErrManifestConfigNotFound)
	})

	s.T().Run("cannot delete an owned config blob", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		manifestDigest, configDigest := RandomDigest(), RandomDigest()

		err := repo.PutBlob(configDigest)
		AssertNoError(t, err)
		err = repo.PutManifest(manifestDigest, store.Manifest{}, store.References{
			Config: configDigest,
		})
		AssertNoError(t, err)

		err = repo.DeleteBlob(configDigest)
		AssertErrorIs(t, err, store.ErrBlobInUse)
	})

	s.T().Run("deletes referenced layers when deleting manifest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		manifestDigest := RandomDigest()

		layerDigests := make([]digest.Digest, 0)
		for range 5 {
			id := RandomDigest()
			layerDigests = append(layerDigests, id)
			err := repo.PutBlob(id)
			AssertNoError(t, err)
		}
		digests := slices.Concat([]digest.Digest{manifestDigest}, layerDigests)

		err := repo.PutManifest(manifestDigest, store.Manifest{}, store.References{
			Layers: layerDigests,
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(manifestDigest)
		AssertNoError(t, err)
		slices.Sort(deleted)
		slices.Sort(digests)
		AssertSlicesEqual(t, deleted, digests)

		for _, digest := range layerDigests {
			err = repo.GetBlob(digest)
			AssertErrorIs(t, err, store.ErrBlobNotFound)
		}
	})

	s.T().Run("does not delete layer referenced by multiple manifests", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		manifestDigestA, manifestDigestB := RandomDigest(), RandomDigest()
		layerDigest := RandomDigest()
		wantDeleted := []digest.Digest{
			manifestDigestB, layerDigest,
		}

		err := repo.PutBlob(layerDigest)
		AssertNoError(t, err)

		err = repo.PutManifest(manifestDigestA, store.Manifest{}, store.References{
			Layers: []digest.Digest{layerDigest},
		})
		AssertNoError(t, err)
		err = repo.PutManifest(manifestDigestB, store.Manifest{}, store.References{
			Layers: []digest.Digest{layerDigest},
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(manifestDigestA)
		AssertNoError(t, err)
		AssertEqual(t, len(deleted), 1).Require()
		AssertEqual(t, deleted[0], manifestDigestA)

		err = repo.GetBlob(layerDigest)
		AssertNoError(t, err)

		deleted, err = repo.DeleteManifest(manifestDigestB)
		AssertNoError(t, err)
		slices.Sort(deleted)
		slices.Sort(wantDeleted)
		AssertSlicesEqual(t, deleted, wantDeleted)

		err = repo.GetBlob(layerDigest)
		AssertErrorIs(t, err, store.ErrBlobNotFound)
	})

	s.T().Run("does not error when deleting manifest with 'duplicate' layers", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		manifestDigest, layerDigest := RandomDigest(), RandomDigest()
		wantDeleted := []digest.Digest{manifestDigest, layerDigest}
		slices.Sort(wantDeleted)

		err := repo.PutBlob(layerDigest)
		AssertNoError(t, err)
		err = repo.PutManifest(manifestDigest, store.Manifest{}, store.References{
			Layers: []digest.Digest{layerDigest, layerDigest},
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(manifestDigest)
		AssertNoError(t, err)
		slices.Sort(deleted)
		AssertSlicesEqual(t, deleted, wantDeleted)
	})

	s.T().Run("deleting referenced layer blob returns ErrBlobInUse", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		manifestDigest, layerDigest := RandomDigest(), RandomDigest()

		err := repo.PutBlob(layerDigest)
		AssertNoError(t, err)
		err = repo.PutManifest(manifestDigest, store.Manifest{}, store.References{
			Layers: []digest.Digest{layerDigest},
		})
		AssertNoError(t, err)

		err = repo.DeleteBlob(layerDigest)
		AssertErrorIs(t, err, store.ErrBlobInUse)
	})

	s.T().Run("creating manifest referencing unknown layer blob returns ErrManifestLayerNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)

		err := repo.PutManifest(RandomDigest(), store.Manifest{}, store.References{
			Layers: []digest.Digest{RandomDigest()},
		})
		AssertErrorIs(t, err, store.ErrManifestInvalid, store.ErrManifestLayerNotFound)
	})

	s.T().Run("deletes listed image manifests when deleting image index", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		indexDigest := RandomDigest()

		imageDigests := make([]digest.Digest, 0)
		for range 5 {
			id := RandomDigest()
			imageDigests = append(imageDigests, id)
			err := repo.PutManifest(id, store.Manifest{}, store.References{})
			AssertNoError(t, err)
		}
		digests := slices.Concat([]digest.Digest{indexDigest}, imageDigests)

		err := repo.PutManifest(indexDigest, store.Manifest{}, store.References{
			Manifests: imageDigests,
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(indexDigest)
		AssertNoError(t, err)
		slices.Sort(deleted)
		slices.Sort(digests)
		AssertSlicesEqual(t, deleted, digests)

		for _, digest := range imageDigests {
			_, err = repo.GetManifest(digest)
			AssertErrorIs(t, err, store.ErrManifestNotFound)
			err = repo.GetBlob(digest)
			AssertErrorIs(t, err, store.ErrBlobNotFound)
		}
	})

	// This case is unlikely to happen under normal circumstances;
	// an image manifest is unlikely to be listed in multiple indices.
	s.T().Run("does not delete image manifests referenced by multiple image indices", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		indexDigestA, indexDigestB := RandomDigest(), RandomDigest()
		imageDigest := RandomDigest()
		digests := []digest.Digest{indexDigestB, imageDigest}
		slices.Sort(digests)

		err := repo.PutManifest(imageDigest, store.Manifest{}, store.References{})
		AssertNoError(t, err)

		err = repo.PutManifest(indexDigestA, store.Manifest{}, store.References{
			Manifests: []digest.Digest{imageDigest},
		})
		AssertNoError(t, err)
		err = repo.PutManifest(indexDigestB, store.Manifest{}, store.References{
			Manifests: []digest.Digest{imageDigest},
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(indexDigestA)
		AssertNoError(t, err)
		AssertEqual(t, len(deleted), 1).Require()
		AssertEqual(t, deleted[0], indexDigestA)

		deleted, err = repo.DeleteManifest(indexDigestB)
		AssertNoError(t, err)
		slices.Sort(deleted)
		AssertSlicesEqual(t, deleted, digests)
	})

	// I do not see this happening in reality, but let's be safe and design defensively.
	s.T().Run("does not error when deleting index with 'duplicate' manifests", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		indexDigest, manifestDigest := RandomDigest(), RandomDigest()
		wantDeleted := []digest.Digest{indexDigest, manifestDigest}
		slices.Sort(wantDeleted)

		err := repo.PutManifest(manifestDigest, store.Manifest{}, store.References{})
		AssertNoError(t, err)
		err = repo.PutManifest(indexDigest, store.Manifest{}, store.References{
			Manifests: []digest.Digest{manifestDigest, manifestDigest},
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(indexDigest)
		AssertNoError(t, err)
		slices.Sort(deleted)
		AssertSlicesEqual(t, deleted, wantDeleted)
	})

	s.T().Run("deleting manifest referenced in index returns ErrManifestInUse", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		indexDigest, imageDigest := RandomDigest(), RandomDigest()

		err := repo.PutManifest(imageDigest, store.Manifest{}, store.References{})
		AssertNoError(t, err)
		err = repo.PutManifest(indexDigest, store.Manifest{}, store.References{
			Manifests: []digest.Digest{imageDigest},
		})
		AssertNoError(t, err)

		_, err = repo.DeleteManifest(imageDigest)
		AssertErrorIs(t, err, store.ErrManifestInUse)
	})

	s.T().Run("creating index referencing unknown manifest returns ErrManifestImageNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)

		err := repo.PutManifest(RandomDigest(), store.Manifest{}, store.References{
			Manifests: []digest.Digest{RandomDigest()},
		})
		AssertErrorIs(t, err, store.ErrManifestInvalid, store.ErrManifestImageNotFound)
	})
}

func (s *MetadataSuite) TestListManifests() {
	if s.SkipListManifests {
		s.T().Skip()
	}

	s.T().Run("lists manifest blobs across repositories", func(t *testing.T) {
		meta := s.Constructor(t)
		want := make([]digest.Digest, 5)
		got := make([]digest.Digest, 0)

		for i := range len(want) {
			name := RandomName()
			repo, err := meta.CreateRepository(name)
			AssertNoError(t, err).Require()

			want[i] = RandomDigest()
			err = repo.PutManifest(want[i], store.Manifest{}, store.References{})
			AssertNoError(t, err).Require()
		}

		for digest := range meta.Blobs() {
			got = append(got, digest)
		}

		slices.Sort(want)
		slices.Sort(got)

		AssertSlicesEqual(t, got, want)
	})

	s.T().Run("does not return deleted manifest blobs", func(t *testing.T) {
		meta := s.Constructor(t)
		name, digest := RandomName(), RandomDigest()
		repo, err := meta.CreateRepository(name)
		AssertNoError(t, err).Require()

		err = repo.PutManifest(digest, store.Manifest{}, store.References{})
		AssertNoError(t, err).Require()

		blobs := slices.Collect(meta.Blobs())
		AssertEqual(t, len(blobs), 1).Require()
		AssertEqual(t, blobs[0], digest)

		_, err = repo.DeleteManifest(digest)
		AssertNoError(t, err).Require()

		blobs = slices.Collect(meta.Blobs())
		AssertNoError(t, err).Require()
		AssertEqual(t, len(blobs), 0)
	})

	s.T().Run("returns a manifest blob deleted in one repository but present in another", func(t *testing.T) {
		meta := s.Constructor(t)
		count := 5
		repos := make([]store.Repository, count)
		digest := RandomDigest()

		var err error
		for i := range repos {
			repos[i], err = meta.CreateRepository(RandomName())
			AssertNoError(t, err).Require()
			err = repos[i].PutManifest(digest, store.Manifest{}, store.References{})
			AssertNoError(t, err).Require()
		}

		for i := range count / 2 {
			_, err = repos[i].DeleteManifest(digest)
			AssertNoError(t, err).Require()
		}

		digests := slices.Collect(meta.Blobs())
		AssertEqual(t, len(digests), 1).Require()
		AssertEqual(t, digests[0], digest)
	})
}

func (s *MetadataSuite) TestReferrers() {
	if s.SkipReferrers {
		s.T().Skip()
	}

	s.T().Run("returns referrers for subject manifest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		subjectDigest := RandomDigest()
		referrerDigests := make([]digest.Digest, 5)
		for i := range referrerDigests {
			referrerDigests[i] = RandomDigest()
		}

		err := repo.PutManifest(subjectDigest, store.Manifest{}, store.References{})
		AssertNoError(t, err)

		for _, referrerDigest := range referrerDigests {
			err := repo.PutManifest(referrerDigest, store.Manifest{}, store.References{
				Subject: subjectDigest,
			})
			AssertNoError(t, err)
		}

		got, err := repo.ListReferrers(subjectDigest)
		AssertNoError(t, err)
		slices.Sort(got)
		slices.Sort(referrerDigests)
		AssertSlicesEqual(t, got, referrerDigests)
	})

	s.T().Run("does not return deleted referrers", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		referrerDigestA, referrerDigestB := RandomDigest(), RandomDigest()
		subjectDigest := RandomDigest()

		err := repo.PutManifest(subjectDigest, store.Manifest{}, store.References{})
		AssertNoError(t, err)

		err = repo.PutManifest(referrerDigestA, store.Manifest{}, store.References{
			Subject: subjectDigest,
		})
		AssertNoError(t, err)

		err = repo.PutManifest(referrerDigestB, store.Manifest{}, store.References{
			Subject: subjectDigest,
		})
		AssertNoError(t, err)

		_, err = repo.DeleteManifest(referrerDigestB)
		AssertNoError(t, err)

		referrers, err := repo.ListReferrers(subjectDigest)
		AssertNoError(t, err)

		AssertEqual(t, len(referrers), 1).Require()
		AssertEqual(t, referrers[0], referrerDigestA)
	})

	s.T().Run("deletes referrers when subject is deleted", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		subjectDigest, referrerDigest := RandomDigest(), RandomDigest()
		digests := []digest.Digest{subjectDigest, referrerDigest}
		slices.Sort(digests)

		err := repo.PutManifest(subjectDigest, store.Manifest{}, store.References{})
		AssertNoError(t, err)
		err = repo.PutManifest(referrerDigest, store.Manifest{}, store.References{
			Subject: subjectDigest,
		})
		AssertNoError(t, err)

		deleted, err := repo.DeleteManifest(subjectDigest)
		AssertNoError(t, err)
		slices.Sort(deleted)
		AssertSlicesEqual(t, deleted, digests)

		_, err = repo.GetManifest(referrerDigest)
		AssertErrorIs(t, err, store.ErrManifestNotFound)
	})

	s.T().Run("rejects manifest with unknown subject with ErrManifestSubjectNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{
			Subject: RandomDigest(),
		})
		AssertErrorIs(t, err, store.ErrManifestInvalid, store.ErrManifestSubjectNotFound)
	})
}

func (s *MetadataSuite) TestTags() {
	if s.SkipTags {
		s.T().Skip()
	}

	s.T().Run("tags a manifest digest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest, tag := RandomDigest(), RandomVersion()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{})
		AssertNoError(t, err)

		err = repo.PutTag(tag, digest)
		AssertNoError(t, err)

		got, err := repo.GetTag(tag)
		AssertNoError(t, err)
		AssertEqual(t, got, digest)
	})

	s.T().Run("deletes an existing tag", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest, tag := RandomDigest(), RandomVersion()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{})
		AssertNoError(t, err)
		err = repo.PutTag(tag, digest)
		AssertNoError(t, err)

		_, err = repo.DeleteTag(tag)
		AssertNoError(t, err)

		_, err = repo.GetTag(tag)
		AssertErrorIs(t, err, store.ErrTagNotFound)
	})

	s.T().Run("deletes the tag's associated manifest", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest, tag := RandomDigest(), RandomVersion()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{})
		AssertNoError(t, err)
		err = repo.PutTag(tag, digest)
		AssertNoError(t, err)

		deleted, err := repo.DeleteTag(tag)
		AssertNoError(t, err)
		AssertEqual(t, len(deleted), 1).Require()
		AssertEqual(t, deleted[0], digest)

		_, err = repo.GetManifest(digest)
		AssertErrorIs(t, err, store.ErrManifestNotFound)
	})

	s.T().Run("deleting tagged manifest returns ErrManifestInUse", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest, tag := RandomDigest(), RandomVersion()

		err := repo.PutManifest(digest, store.Manifest{}, store.References{})
		AssertNoError(t, err)
		err = repo.PutTag(tag, digest)
		AssertNoError(t, err)

		_, err = repo.DeleteManifest(digest)
		AssertErrorIs(t, err, store.ErrManifestInUse)
	})

	s.T().Run("deleting unknown tag returns ErrTagNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)

		_, err := repo.DeleteTag(RandomVersion())
		AssertErrorIs(t, err, store.ErrTagNotFound)
	})

	s.T().Run("tagging unknown manifest returns ErrManifestNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)

		err := repo.PutTag(RandomVersion(), RandomDigest())
		AssertErrorIs(t, err, store.ErrManifestNotFound)
	})

	s.T().Run("tagging blob returns ErrManifestNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)
		digest := RandomDigest()

		err := repo.PutBlob(digest)
		AssertNoError(t, err)

		err = repo.PutTag(RandomVersion(), digest)
		AssertErrorIs(t, err, store.ErrManifestNotFound)
	})
}

func (s *MetadataSuite) TestListTags() {
	if s.SkipListTags {
		s.T().Skip()
	}

	tags := []string{
		"v1.0.32", "v1.17.56", "v1.3.5", "v3.9.30", "v4.12.14",
		"v4.6.49", "v1.17.32", "v0.5.9", "v3.6.17", "v4.8.4",
	}
	sortedTags := []string{
		"v0.5.9", "v1.0.32", "v1.17.32", "v1.17.56", "v1.3.5",
		"v3.6.17", "v3.9.30", "v4.12.14", "v4.6.49", "v4.8.4",
	}

	tc := []struct {
		name  string
		count int
		last  string
		want  []string
		err   error
	}{
		{
			name:  "returns all tags in lexical order",
			count: -1,
			last:  "",
			want:  sortedTags,
		},
		{
			name:  "returns number of tags equal to count param",
			count: 5,
			last:  "",
			want:  []string{"v0.5.9", "v1.0.32", "v1.17.32", "v1.17.56", "v1.3.5"},
		},
		{
			name:  "returns empty list when count param is 0",
			count: 0,
			last:  "",
			want:  []string{},
		},
		{
			name:  "returns all tags when count param is greater than number of tags",
			count: 20,
			last:  "",
			want:  sortedTags,
		},
		{
			name:  "only returns tags after tag given as last",
			count: 3,
			last:  "v3.9.30",
			want:  []string{"v4.12.14", "v4.6.49", "v4.8.4"},
		},
		{
			name:  "count may be set to -1 to return all tags after last tag",
			count: -1,
			last:  "v1.3.5",
			want:  []string{"v3.6.17", "v3.9.30", "v4.12.14", "v4.6.49", "v4.8.4"},
		},
		{
			name:  "returns ErrTagNotFound when last is not found",
			count: -1,
			last:  "v0.0.0",
			err:   store.ErrTagNotFound,
		},
		{
			name:  "count in combination with last does not go out of bounds",
			count: 3,
			last:  "v4.12.14",
			want:  []string{"v4.6.49", "v4.8.4"},
		},
	}

	for _, tt := range tc {
		s.T().Run(tt.name, func(t *testing.T) {
			repo := s.RepositoryConstructor(t)
			digest := RandomDigest()

			err := repo.PutManifest(digest, store.Manifest{}, store.References{})
			AssertNoError(t, err).Require()

			for _, tag := range tags {
				err := repo.PutTag(tag, digest)
				AssertNoError(t, err).Require()
			}

			got, err := repo.ListTags(tt.count, tt.last)
			AssertErrorIs(t, err, tt.err)
			AssertSlicesEqual(t, got, tt.want)
		})
	}
}

func (s *MetadataSuite) TestUploadSessions() {
	if s.SkipUploadSessions {
		s.T().Skip()
	}

	session := &store.UploadSession{
		ID:        RandomUUID(),
		StartDate: time.Now().UTC().Round(0),
		HashState: RandomBytes(8),
	}

	s.T().Run("creates new session", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)

		_, err := repo.GetUploadSession(session.ID)
		AssertErrorIs(t, err, store.ErrUploadNotFound)

		err = repo.PutUploadSession(session)
		AssertNoError(t, err)

		got, err := repo.GetUploadSession(session.ID)
		AssertNoError(t, err)
		AssertDeepEqual(t, got, session)
	})

	s.T().Run("deletes an existing session", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)

		err := repo.PutUploadSession(session)
		AssertNoError(t, err)

		_, err = repo.GetUploadSession(session.ID)
		AssertNoError(t, err)

		err = repo.DeleteUploadSession(session.ID)
		AssertNoError(t, err)

		_, err = repo.GetUploadSession(session.ID)
		AssertErrorIs(t, err, store.ErrUploadNotFound)
	})

	s.T().Run("deleting unknown session returns ErrUploadNotFound", func(t *testing.T) {
		repo := s.RepositoryConstructor(t)

		err := repo.DeleteUploadSession(RandomUUID())
		AssertErrorIs(t, err, store.ErrUploadNotFound)
	})
}

func (s *MetadataSuite) TestRecursiveGC() {
	if s.SkipRecursiveGC {
		s.T().Skip()
	}

	s.T().Run("deletes all blobs of tagged image manifest with referrer", func(t *testing.T) {
		meta := s.Constructor(t)
		name := RandomName()
		repo, err := meta.CreateRepository(name)
		AssertNoError(t, err)

		configDigest := RandomDigest()
		err = repo.PutBlob(configDigest)
		AssertNoError(t, err)

		layerDigests := make([]digest.Digest, 5)
		for i := range layerDigests {
			layerDigests[i] = RandomDigest()
			err = repo.PutBlob(layerDigests[i])
			AssertNoError(t, err)
		}

		manifestDigest := RandomDigest()
		err = repo.PutManifest(manifestDigest, store.Manifest{}, store.References{
			Config: configDigest,
			Layers: layerDigests,
		})
		AssertNoError(t, err)

		referrerDigest := RandomDigest()
		err = repo.PutManifest(referrerDigest, store.Manifest{}, store.References{
			Subject: manifestDigest,
		})
		AssertNoError(t, err)

		tag := RandomVersion()
		err = repo.PutTag(tag, manifestDigest)
		AssertNoError(t, err)

		allDigests := slices.Concat(layerDigests, []digest.Digest{configDigest, manifestDigest, referrerDigest})
		slices.Sort(allDigests)

		blobs := slices.Collect(meta.Blobs())
		slices.Sort(blobs)
		AssertSlicesEqual(t, allDigests, blobs)

		deleted, err := repo.DeleteTag(tag)
		AssertNoError(t, err)
		slices.Sort(deleted)
		AssertSlicesEqual(t, allDigests, deleted)

		blobs = slices.Collect(meta.Blobs())
		AssertEqual(t, len(blobs), 0)
	})
}

func (s *MetadataSuite) TestSnapshotRestore() {
	if s.SkipSnapshotRestore {
		s.T().Skip()
	}

	name, digest := RandomName(), RandomDigest()

	s.T().Run("snapshot and restore into another MetadataStore", func(t *testing.T) {
		// The store that we're taking a snapshot from.
		snapshotStore := s.Constructor(t)
		// The store that we're restoring into.
		restoreStore := s.Constructor(t)

		repo, err := snapshotStore.CreateRepository(name)
		AssertNoError(t, err).Require()

		err = repo.PutBlob(digest)
		AssertNoError(t, err).Require()

		snapshot := new(bytes.Buffer)
		err = snapshotStore.Snapshot(snapshot)
		AssertNoError(t, err).Require()

		err = restoreStore.Restore(snapshot)
		AssertNoError(t, err).Require()

		repo, err = restoreStore.GetRepository(name)
		AssertNoError(t, err).Require()

		err = repo.GetBlob(digest)
		AssertNoError(t, err).Require()
	})

	s.T().Run("snapshot and restore in-place", func(t *testing.T) {
		meta := s.Constructor(t)
		snapshot := new(bytes.Buffer)

		repo, err := meta.CreateRepository(name)
		AssertNoError(t, err).Require()
		err = repo.PutBlob(digest)
		AssertNoError(t, err).Require()

		err = repo.GetBlob(digest)
		AssertNoError(t, err).Require()

		err = meta.Snapshot(snapshot)
		AssertNoError(t, err).Require()

		err = repo.DeleteBlob(digest)
		AssertNoError(t, err).Require()

		err = repo.GetBlob(digest)
		AssertErrorIs(t, err, store.ErrBlobNotFound)

		err = meta.Restore(snapshot)
		AssertNoError(t, err).Require()

		repo, err = meta.GetRepository(name)
		AssertNoError(t, err).Require()

		err = repo.GetBlob(digest)
		AssertNoError(t, err).Require()
	})
}
