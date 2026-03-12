package repository

import (
	"testing"

	"github.com/robinkb/cascade-registry/registry/store"
	. "github.com/robinkb/cascade-registry/testing"
)

func (s *Suite) TestStatManifest() {
	if s.Tests.ManifestsDisbabled {
		s.T().SkipNow()
	}

	name := s.RandomRepository()
	digest, _, content := RandomManifest()

	err := s.metadata.PutManifest(name, digest, &store.ManifestMetadata{})
	RequireNoError(s.T(), err)
	err = s.blobs.PutBlob(digest, content)
	RequireNoError(s.T(), err)

	s.T().Run("Returns FileInfo with expected size on known manifest", func(t *testing.T) {
		info, err := s.repository.StatManifest(name, digest.String())
		RequireNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("got size of %d, expected %d", got, want)
		}
		AssertNoError(t, err)
	})

	s.T().Run("Returns ErrManifestUnkonwn on known manifest in other repository", func(t *testing.T) {
		_, err := s.repository.StatManifest("unknown/repository", digest.String())
		AssertErrorIs(t, err, ErrManifestUnknown)
	})

	s.T().Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := s.repository.StatManifest(name, "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		AssertErrorIs(t, err, ErrManifestUnknown)
	})

	s.T().Run("returns ErrManifestUnknown on invalid digest", func(t *testing.T) {
		_, err := s.repository.StatManifest(name, "sha256:i-am-not-valid-lol")
		AssertErrorIs(t, err, ErrManifestUnknown)
	})
}

func (s *Suite) TestGetManifest() {
	if s.Tests.ManifestsDisbabled {
		s.T().SkipNow()
	}

	name := s.RandomRepository()
	digest, _, content := RandomManifest()

	err := s.metadata.PutManifest(name, digest, &store.ManifestMetadata{})
	RequireNoError(s.T(), err)
	err = s.blobs.PutBlob(digest, content)
	RequireNoError(s.T(), err)

	s.T().Run("Retrieve an existing manifest", func(t *testing.T) {
		_, got, err := s.repository.GetManifest(name, digest.String())
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, content)
	})

	s.T().Run("Metadata is correctly retrieved", func(t *testing.T) {
		name := s.RandomRepository()
		want := store.ManifestMetadata{
			Annotations: map[string]string{
				RandomString(6): RandomString(32),
			},
			ArtifactType: RandomString(6),
			MediaType:    RandomString(6),
			Size:         42,
			// Subject field is not persisted, only used for creating links.
		}

		err := s.metadata.PutManifest(name, digest, &want)
		RequireNoError(t, err)
		err = s.blobs.PutBlob(digest, content)
		RequireNoError(t, err)

		got, _, err := s.repository.GetManifest(name, digest.String())
		AssertNoError(t, err)
		AssertDeepEqual(t, got, &want)
	})

	s.T().Run("Returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, _, err := s.repository.GetManifest(name, "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		AssertErrorIs(t, err, ErrManifestUnknown)
	})

	s.T().Run("Returns ErrNameUnknown on unknown repository", func(t *testing.T) {
		_, _, err := s.repository.GetManifest(RandomName(), RandomDigest().String())
		AssertErrorIs(t, err, ErrNameUnknown)
	})
}

func (s *Suite) TestPutManifest() {
	if s.Tests.ManifestsDisbabled {
		s.T().SkipNow()
	}

	name := s.RandomRepository()

	s.T().Run("Put and retrieve a manifest", func(t *testing.T) {
		digest, _, content := RandomManifest()

		_, err := s.repository.PutManifest(name, digest.String(), content)
		AssertNoError(t, err)

		_, got, err := s.repository.GetManifest(name, digest.String())
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, content)
	})

	s.T().Run("Putting a manifest with subject returns the subject hash", func(t *testing.T) {
		subjDigest, subjManifest, subjContent := RandomManifest()
		digest, _, content := RandomManifestWithSubject(subjDigest, subjManifest)

		_, err := s.repository.PutManifest(name, subjDigest.String(), subjContent)
		RequireNoError(t, err)

		gotSubject, err := s.repository.PutManifest(name, digest.String(), content)
		AssertNoError(t, err)
		AssertEqual(t, gotSubject, subjDigest)
	})

	s.T().Run("Putting a manifest with subject that points to an unknown blob does not error", func(t *testing.T) {
		// This is actually still up for debate in the specification.
		// See: https://github.com/opencontainers/distribution-spec/issues/459
		subjDigest, subjManifest, _ := RandomManifest()
		digest, _, content := RandomManifestWithSubject(subjDigest, subjManifest)

		_, err := s.repository.PutManifest(name, digest.String(), content)
		AssertNoError(t, err)
	})

	s.T().Run("Putting a manifest with invalid content returns ErrManifestInvalid", func(t *testing.T) {
		digest, content := RandomBlob(32)

		_, err := s.repository.PutManifest(name, digest.String(), content)
		AssertErrorIs(t, err, ErrManifestInvalid)
	})

	s.T().Run("Putting a manifest into an unknown repository returns ErrNameUnknown", func(t *testing.T) {
		name := RandomName()
		digest, _, content := RandomManifest()

		_, err := s.repository.PutManifest(name, digest.String(), content)
		AssertErrorIs(t, err, ErrNameUnknown)
	})
}

func (s *Suite) TestDeleteManifest() {
	if s.Tests.ManifestsDisbabled {
		s.T().SkipNow()
	}

	s.T().Run("Delete manifest and make sure it cannot be retrieved", func(t *testing.T) {
		name := s.RandomRepository()
		digest, _, content := RandomManifest()

		_, err := s.repository.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		_, err = s.repository.StatManifest(name, digest.String())
		RequireNoError(t, err)

		err = s.repository.DeleteManifest(name, digest.String())
		AssertNoError(t, err)

		_, err = s.repository.StatManifest(name, digest.String())
		AssertErrorIs(t, err, ErrManifestUnknown)
	})

	s.T().Run("Deleting an unknown manifest returns ErrManifestUknown", func(t *testing.T) {
		err := s.repository.DeleteManifest("does/not/exist", "123")
		AssertErrorIs(t, err, ErrManifestUnknown)
	})
}
