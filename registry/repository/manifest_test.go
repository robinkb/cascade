package repository

import (
	"testing"

	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	mockstore "github.com/robinkb/cascade/testing/mock/store"
)

func TestPutManifest(t *testing.T) {
	t.Run("processes an image manifest", func(t *testing.T) {
		manifest := NewImageManifestBuilder(t).
			WithLayers(5).
			Build()

		blobs := mockstore.NewBlobs(t)
		blobs.EXPECT().
			PutBlob(manifest.Digest, manifest.Bytes).
			Return(nil)

		repo := mockstore.NewRepository(t)
		repo.EXPECT().
			PutManifest(manifest.Digest, manifest.Metadata(), manifest.References()).
			Return(nil)

		svc := NewRepositoryService(blobs, repo)
		subj, err := svc.PutManifest(manifest.Digest.String(), manifest.Bytes)
		AssertEqual(t, subj, "")
		AssertNoError(t, err)
	})

	t.Run("processes a manifest with subject", func(t *testing.T) {
		subject := NewImageManifestBuilder(t).Build()
		referrer := NewImageManifestBuilder(t).WithSubject(subject.Manifest).Build()

		blobs := mockstore.NewBlobs(t)
		blobs.EXPECT().
			PutBlob(referrer.Digest, referrer.Bytes).
			Return(nil)

		repo := mockstore.NewRepository(t)
		repo.EXPECT().
			PutManifest(referrer.Digest, referrer.Metadata(), referrer.References()).
			Return(nil)

		svc := NewRepositoryService(blobs, repo)
		id, err := svc.PutManifest(referrer.Digest.String(), referrer.Bytes)
		AssertNoError(t, err)
		AssertEqual(t, id, subject.Digest)
	})

	t.Run("processes an image index", func(t *testing.T) {})
}
