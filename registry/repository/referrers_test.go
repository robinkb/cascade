package repository

import (
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	. "github.com/robinkb/cascade/testing"
)

func (s *Suite) TestListReferrers() {
	if s.Tests.ReferrersDisabled {
		s.T().SkipNow()
	}

	name := s.RandomRepository()

	digest, _, content := RandomManifest()
	wantIndex, referrers := GenerateReferrersWithIndex(s.T(), digest)
	wantFilteredIndex := &v1.Index{
		Manifests: []v1.Descriptor{
			wantIndex.Manifests[0],
		},
	}

	_, err := s.repository.PutManifest(name, digest.String(), content)
	RequireNoError(s.T(), err)

	for _, referrer := range referrers {
		_, err = s.repository.PutManifest(name, referrer.Digest.String(), referrer.Content)
		RequireNoError(s.T(), err)
	}

	s.T().Run("Fetch full list of referrers", func(t *testing.T) {
		got, err := s.repository.ListReferrers(name, digest.String(), nil)
		RequireNoError(t, err)
		AssertIndex(t, got.Index, wantIndex)
		AssertSlicesEqual(t, got.AppliedFilters, []string{})
	})

	s.T().Run("Fetch filtered list of referrers", func(t *testing.T) {
		got, err := s.repository.ListReferrers(name, digest.String(), &ListReferrersOptions{
			ArtifactType: "application/vnd.example+type",
		})
		RequireNoError(t, err)
		AssertIndex(t, got.Index, wantFilteredIndex)
		AssertSlicesEqual(t, got.AppliedFilters, []string{"artifactType"})
	})

	s.T().Run("List referrers on existing manifest without referrers", func(t *testing.T) {
		digest, _, content := RandomManifest()

		_, err := s.repository.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		got, err := s.repository.ListReferrers(name, digest.String(), nil)
		RequireNoError(t, err)
		AssertEqual(t, len(got.Index.Manifests), 0)
	})

	s.T().Run("List referrers when subject manifest is pushed last", func(t *testing.T) {
		digest, _, content := RandomManifest()
		wantIndex, referrers := GenerateReferrersWithIndex(t, digest)

		for _, referrer := range referrers {
			_, err = s.repository.PutManifest(name, referrer.Digest.String(), referrer.Content)
			RequireNoError(t, err)
		}

		_, err := s.repository.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		got, err := s.repository.ListReferrers(name, digest.String(), nil)
		RequireNoError(t, err)
		AssertIndex(t, got.Index, wantIndex)
	})

	s.T().Run("List referrers on known repository but on unknown manifest", func(t *testing.T) {
		digest, _, content := RandomManifest()

		_, err := s.repository.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		digest = RandomDigest()
		_, err = s.repository.ListReferrers(name, digest.String(), nil)
		AssertNoError(t, err)
	})

	s.T().Run("List referrers on non-existent repository", func(t *testing.T) {
		name, digest := RandomName(), RandomDigest()

		_, err := s.repository.ListReferrers(name, digest.String(), nil)
		AssertErrorIs(t, err, ErrNameUnknown)
	})
}
