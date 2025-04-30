package cascade_test

import (
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestListReferrers(t *testing.T) {
	service, _, _ := newTestRegistry()

	name := RandomName()

	subjectDigest, _, subjectContent := RandomManifest()
	referrerManifest := v1.Manifest{
		Subject: &v1.Descriptor{
			Digest: subjectDigest,
		},
	}
	referrerContent, _ := json.Marshal(&referrerManifest)
	referrerDigest := digest.FromBytes(referrerContent)

	_, err := service.PutManifest(name, subjectDigest.String(), subjectContent)
	RequireNoError(t, err)

	_, err = service.PutManifest(name, referrerDigest.String(), referrerContent)
	RequireNoError(t, err)

	t.Run("Fetch full list of referrers", func(t *testing.T) {
		idx, err := service.ListReferrers(name, subjectDigest.String())
		AssertNoError(t, err)

		if len(idx.Manifests) != 1 {
			t.Fatalf("unexpected count of descriptors")
		}

		if idx.Manifests[0].Digest != referrerDigest {
			t.Errorf("wrong digest")
		}
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
