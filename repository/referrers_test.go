package repository_test

import (
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry/repository"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestListReferrers(t *testing.T) {
	service, _, _ := newTestRepository()

	name := RandomName()

	digest, _, content := RandomManifest()
	wantIndex, referrers := GenerateReferrersWithIndex(t, digest)
	wantFilteredIndex := &v1.Index{
		Manifests: []v1.Descriptor{
			wantIndex.Manifests[0],
		},
	}

	_, err := service.PutManifest(name, digest.String(), content)
	RequireNoError(t, err)

	for _, referrer := range referrers {
		_, err = service.PutManifest(name, referrer.Digest.String(), referrer.Content)
		RequireNoError(t, err)
	}

	t.Run("Fetch full list of referrers", func(t *testing.T) {
		got, err := service.ListReferrers(name, digest.String(), nil)
		AssertNoError(t, err)
		AssertIndex(t, got.Index, wantIndex)
		AssertSlicesEqual(t, got.AppliedFilters, []string{})
	})

	t.Run("Fetch filtered list of referrers", func(t *testing.T) {
		got, err := service.ListReferrers(name, digest.String(), &repository.ListReferrersOptions{
			ArtifactType: "application/vnd.example+type",
		})
		AssertNoError(t, err)
		AssertIndex(t, got.Index, wantFilteredIndex)
		AssertSlicesEqual(t, got.AppliedFilters, []string{"artifactType"})
	})

	t.Run("List referrers on existing manifest without referrers", func(t *testing.T) {
		name := RandomName()
		digest, _, content := RandomManifest()

		_, err := service.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		got, err := service.ListReferrers(name, digest.String(), nil)
		AssertNoError(t, err)
		AssertEqual(t, len(got.Index.Manifests), 0)
	})

	t.Run("List referrers when subject manifest is pushed last", func(t *testing.T) {
		name := RandomName()

		digest, _, content := RandomManifest()
		wantIndex, referrers := GenerateReferrersWithIndex(t, digest)

		for _, referrer := range referrers {
			_, err = service.PutManifest(name, referrer.Digest.String(), referrer.Content)
			RequireNoError(t, err)
		}

		_, err := service.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		got, err := service.ListReferrers(name, digest.String(), nil)
		AssertNoError(t, err)
		AssertIndex(t, got.Index, wantIndex)
	})

	t.Run("List referrers on known repository but on unknown manifest", func(t *testing.T) {
		name := RandomName()
		digest, _, content := RandomManifest()

		_, err := service.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		digest = RandomDigest()
		_, err = service.ListReferrers(name, digest.String(), nil)
		AssertNoError(t, err)
	})

	t.Run("List referrers on non-existent repository", func(t *testing.T) {
		name, digest := RandomName(), RandomDigest()

		_, err := service.ListReferrers(name, digest.String(), nil)
		AssertErrorIs(t, err, repository.ErrNameUnknown)
	})
}
