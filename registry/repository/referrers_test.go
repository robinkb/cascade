package repository

import (
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	. "github.com/robinkb/cascade/testing"
)

func TestListReferrers(t *testing.T) {
	_, _, svc := NewTestRepository(t)

	digest, _, content := RandomManifest()
	wantIndex, referrers := GenerateReferrersWithIndex(t, digest)
	wantFilteredIndex := &v1.Index{
		Manifests: []v1.Descriptor{
			wantIndex.Manifests[0],
		},
	}

	_, err := svc.PutManifest(digest.String(), content)
	RequireNoError(t, err)

	for _, referrer := range referrers {
		_, err = svc.PutManifest(referrer.Digest.String(), referrer.Content)
		RequireNoError(t, err)
	}

	t.Run("Fetch full list of referrers", func(t *testing.T) {
		got, err := svc.ListReferrers(digest.String(), nil)
		RequireNoError(t, err)
		AssertIndex(t, got.Index, wantIndex)
		AssertSlicesEqual(t, got.AppliedFilters, []string{})
	})

	t.Run("Fetch filtered list of referrers", func(t *testing.T) {
		got, err := svc.ListReferrers(digest.String(), &ListReferrersOptions{
			ArtifactType: "application/vnd.example+type",
		})
		RequireNoError(t, err)
		AssertIndex(t, got.Index, wantFilteredIndex)
		AssertSlicesEqual(t, got.AppliedFilters, []string{"artifactType"})
	})

	t.Run("List referrers on existing manifest without referrers", func(t *testing.T) {
		digest, _, content := RandomManifest()

		_, err := svc.PutManifest(digest.String(), content)
		RequireNoError(t, err)

		got, err := svc.ListReferrers(digest.String(), nil)
		RequireNoError(t, err)
		AssertEqual(t, len(got.Index.Manifests), 0)
	})

	t.Run("List referrers when subject manifest is pushed last", func(t *testing.T) {
		digest, _, content := RandomManifest()
		wantIndex, referrers := GenerateReferrersWithIndex(t, digest)

		_, err := svc.PutManifest(digest.String(), content)
		RequireNoError(t, err)

		for _, referrer := range referrers {
			_, err = svc.PutManifest(referrer.Digest.String(), referrer.Content)
			RequireNoError(t, err)
		}

		got, err := svc.ListReferrers(digest.String(), nil)
		RequireNoError(t, err)
		AssertIndex(t, got.Index, wantIndex)
	})

	t.Run("List referrers on known repository but on unknown manifest", func(t *testing.T) {
		digest, _, content := RandomManifest()

		_, err := svc.PutManifest(digest.String(), content)
		RequireNoError(t, err)

		digest = RandomDigest()
		_, err = svc.ListReferrers(digest.String(), nil)
		AssertNoError(t, err)
	})

	// TODO: Adapt this.
	// t.Run("List referrers on non-existent repository", func(t *testing.T) {
	// 	name, digest := RandomName(), RandomDigest()

	// 	_, err := svc.ListReferrers(digest.String(), nil)
	// 	AssertErrorIs(t, err, ErrNameUnknown)
	// })
}
