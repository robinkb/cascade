package repository

import (
	"testing"

	"github.com/opencontainers/go-digest"
	. "github.com/robinkb/cascade/testing"
	. "github.com/robinkb/cascade/testing/repository"
	"github.com/robinkb/cascade/testing/store/mock"
)

func TestPutTag(t *testing.T) {
	t.Run("deletes garbage collected blobs", func(t *testing.T) {
		manifest := NewImageManifestBuilder(t).Build()
		tag := RandomVersion()

		repo := mock.NewRepository(t)
		repo.EXPECT().
			PutTag(tag, manifest.Digest).
			Return([]digest.Digest{manifest.Digest}, nil)

		blobs := mock.NewBlobs(t)
		blobs.EXPECT().
			DeleteBlob(manifest.Digest).
			Return(nil)

		svc := New(blobs, repo)
		err := svc.PutTag(tag, manifest.Digest.String())
		AssertNoError(t, err)
	})
}

func TestDeleteTags(t *testing.T) {
	t.Run("deletes garbage collected blobs", func(t *testing.T) {
		manifest := NewImageManifestBuilder(t).WithLayers(5).Build()
		wantDeleted := append(manifest.LayersAsDigests(), manifest.Digest)
		tag := RandomVersion()

		repo := mock.NewRepository(t)
		repo.EXPECT().
			DeleteTag(tag).
			Return(wantDeleted, nil)

		blobs := mock.NewBlobs(t)
		for _, id := range wantDeleted {
			blobs.EXPECT().
				DeleteBlob(id).
				Return(nil)
		}

		svc := New(blobs, repo)
		err := svc.DeleteTag(tag)
		AssertNoError(t, err)
	})

	t.Run("deduplicates deleted blobs before deletion", func(t *testing.T) {
		id := RandomDigest()
		digests := []digest.Digest{id, id, id}
		tag := RandomVersion()

		repo := mock.NewRepository(t)
		repo.EXPECT().
			DeleteTag(tag).
			Return(digests, nil)

		blobs := mock.NewBlobs(t)
		blobs.EXPECT().
			DeleteBlob(id).
			Return(nil).Once()

		svc := New(blobs, repo)
		err := svc.DeleteTag(tag)
		AssertNoError(t, err)
	})
}
