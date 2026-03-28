package repository

import (
	"io"
	"testing"

	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing"
)

func TestStatManifest(t *testing.T) {
	blobs, repo, svc := NewTestRepository(t)

	digest, _, content := RandomManifest()

	err := repo.PutManifest(digest, store.Manifest{}, store.References{})
	RequireNoError(t, err)
	err = blobs.PutBlob(digest, content)
	RequireNoError(t, err)

	t.Run("Returns FileInfo with expected size on known manifest", func(t *testing.T) {
		info, err := svc.StatManifest(digest.String())
		RequireNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("got size of %d, expected %d", got, want)
		}
		AssertNoError(t, err)
	})

	t.Run("Returns ErrManifestUnkonwn on known manifest in other repository", func(t *testing.T) {
		_, err := svc.StatManifest(digest.String())
		AssertErrorIs(t, err, ErrManifestUnknown)
	})

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := svc.StatManifest("sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		AssertErrorIs(t, err, ErrManifestUnknown)
	})

	t.Run("returns ErrManifestUnknown on invalid digest", func(t *testing.T) {
		_, err := svc.StatManifest("sha256:i-am-not-valid-lol")
		AssertErrorIs(t, err, ErrManifestUnknown)
	})
}

func TestGetManifest(t *testing.T) {
	blobs, repo, svc := NewTestRepository(t)

	digest, _, content := RandomManifest()

	err := repo.PutManifest(digest, store.Manifest{}, store.References{})
	RequireNoError(t, err)
	err = blobs.PutBlob(digest, content)
	RequireNoError(t, err)

	t.Run("Retrieve an existing manifest", func(t *testing.T) {
		_, got, err := svc.GetManifest(digest.String())
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, content)
	})

	t.Run("Metadata is correctly retrieved", func(t *testing.T) {
		want := store.Manifest{
			Annotations: map[string]string{
				RandomString(6): RandomString(32),
			},
			ArtifactType: RandomString(6),
			MediaType:    RandomString(6),
			Size:         42,
		}

		err := repo.PutManifest(digest, want, store.References{})
		RequireNoError(t, err)
		err = blobs.PutBlob(digest, content)
		RequireNoError(t, err)

		got, _, err := svc.GetManifest(digest.String())
		AssertNoError(t, err)
		AssertDeepEqual(t, got, &want)
	})

	t.Run("Returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, _, err := svc.GetManifest("sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		AssertErrorIs(t, err, ErrManifestUnknown)
	})
}

func TestPutManifest(t *testing.T) {
	_, _, svc := NewTestRepository(t)

	t.Run("Put and retrieve a manifest", func(t *testing.T) {
		digest, _, content := RandomManifest()

		_, err := svc.PutManifest(digest.String(), content)
		AssertNoError(t, err)

		_, got, err := svc.GetManifest(digest.String())
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, content)

		r, err := svc.GetBlob(digest.String())
		AssertNoError(t, err).Require()

		gotBlob, err := io.ReadAll(r)
		AssertSlicesEqual(t, gotBlob, content)
	})

	t.Run("Putting a manifest with subject returns the subject hash", func(t *testing.T) {
		subjDigest, subjManifest, subjContent := RandomManifest()
		digest, _, content := RandomManifestWithSubject(subjDigest, subjManifest)

		_, err := svc.PutManifest(subjDigest.String(), subjContent)
		RequireNoError(t, err)

		gotSubject, err := svc.PutManifest(digest.String(), content)
		AssertNoError(t, err)
		AssertEqual(t, gotSubject, subjDigest)
	})

	t.Run("Putting a manifest with subject that points to an unknown blob does not error", func(t *testing.T) {
		// This is actually still up for debate in the specification.
		// See: https://github.com/opencontainers/distribution-spec/issues/459
		t.Skip()
		subjDigest, subjManifest, _ := RandomManifest()
		digest, _, content := RandomManifestWithSubject(subjDigest, subjManifest)

		_, err := svc.PutManifest(digest.String(), content)
		AssertNoError(t, err)
	})

	t.Run("Putting a manifest with invalid content returns ErrManifestInvalid", func(t *testing.T) {
		digest, content := RandomBlob(32)

		_, err := svc.PutManifest(digest.String(), content)
		AssertErrorIs(t, err, ErrManifestInvalid)
	})
}

// func (s *Suite) TestDeleteManifest() {
// 	if s.Tests.ManifestsDisbabled {
// 		s.T().SkipNow()
// 	}

// 	s.T().Run("Delete manifest and make sure it cannot be retrieved", func(t *testing.T) {
// 		name := s.RandomRepository()
// 		digest, _, content := RandomManifest()

// 		_, err := s.repository.PutManifest(name, digest.String(), content)
// 		RequireNoError(t, err)

// 		_, err = s.repository.StatManifest(name, digest.String())
// 		RequireNoError(t, err)

// 		err = s.repository.DeleteManifest(name, digest.String())
// 		AssertNoError(t, err)

// 		_, err = s.repository.StatManifest(name, digest.String())
// 		AssertErrorIs(t, err, ErrManifestUnknown)
// 	})

// 	s.T().Run("Deleting an unknown manifest returns ErrManifestUknown", func(t *testing.T) {
// 		err := s.repository.DeleteManifest("does/not/exist", "123")
// 		AssertErrorIs(t, err, ErrManifestUnknown)
// 	})
// }
