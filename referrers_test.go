package cascade_test

import (
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestListReferrers(t *testing.T) {
	service, _, _ := newTestRegistry()

	repository := RandomName()
	subjectManifest, subjectDigest := RandomManifest()

	_, err := service.PutManifest(repository, subjectDigest.String(), subjectManifest.Bytes())
	RequireNoError(t, err)

	t.Run("Listing referrers returns the expected index for the given subject", func(t *testing.T) {
		referrerCount := 3
		referrers := make([]v1.Descriptor, referrerCount)
		for i := range referrerCount {
			manifest, digest := RandomManifestWithSubject(subjectManifest)
			referrers[i] = v1.Descriptor{
				ArtifactType: manifest.ArtifactType,
				Digest:       digest,
				Size:         int64(len(manifest.Bytes())),
				Annotations:  manifest.Annotations,
			}

			_, err := service.PutManifest(repository, digest.String(), manifest.Bytes())
			RequireNoError(t, err)
		}

		idx, err := service.ListReferrers(repository, subjectDigest.String())
		AssertNoError(t, err)

		AssertEqual(t, idx.MediaType, "application/vnd.oci.image.index.v1+json")
		AssertIndexContainsReferrers(t, idx, referrers...)
	})

	t.Run("Listed referrer ", func(t *testing.T) {
	})

	t.Run("Invalid digest returns error", func(t *testing.T) {
		repository := RandomName()
		digest := "12345"

		_, err := service.ListReferrers(repository, digest)

		AssertErrorIs(t, err, cascade.ErrDigestInvalid)
	})
}
