package cascade_test

import (
	"testing"

	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestListReferrers(t *testing.T) {
	service, _, _ := newTestRegistry()

	name := RandomName()

	digest, _, content := RandomManifest()
	wantIndex, referrers := GenerateReferrersWithIndex(t, digest)

	_, err := service.PutManifest(name, digest.String(), content)
	RequireNoError(t, err)

	for _, referrer := range referrers {
		_, err = service.PutManifest(name, referrer.Digest.String(), referrer.Content)
		RequireNoError(t, err)
	}

	t.Run("Fetch full list of referrers", func(t *testing.T) {
		gotIndex, err := service.ListReferrers(name, digest.String())
		AssertNoError(t, err)
		AssertIndex(t, gotIndex, wantIndex)
	})

	t.Run("List referrers on existing manifest without referrers", func(t *testing.T) {
		name := RandomName()
		digest, _, content := RandomManifest()

		_, err := service.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		idx, err := service.ListReferrers(name, digest.String())
		AssertNoError(t, err)
		AssertEqual(t, len(idx.Manifests), 0)
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

		gotIndex, err := service.ListReferrers(name, digest.String())
		AssertIndex(t, gotIndex, wantIndex)
	})

	t.Run("List referrers on known repository but on unknown manifest", func(t *testing.T) {
		name := RandomName()
		digest, _, content := RandomManifest()

		_, err := service.PutManifest(name, digest.String(), content)
		RequireNoError(t, err)

		digest = RandomDigest()
		_, err = service.ListReferrers(name, digest.String())
		AssertNoError(t, err)
	})

	t.Run("List referrers on non-existent repository", func(t *testing.T) {
		name, digest := RandomName(), RandomDigest()

		_, err := service.ListReferrers(name, digest.String())
		AssertErrorIs(t, err, cascade.ErrNameUnknown)
	})
}
