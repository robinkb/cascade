package repository

import (
	"testing"

	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	mockstore "github.com/robinkb/cascade/testing/mock/store"
)

func TestDeleteTags(t *testing.T) {
	t.Run("deletes garbage collected blobs", func(t *testing.T) {
		manifest := NewImageManifestBuilder(t).WithLayers(5).Build()
		wantDeleted := append(manifest.LayersAsDigests(), manifest.Digest)
		tag := RandomVersion()

		repo := mockstore.NewRepository(t)
		repo.EXPECT().
			DeleteTag(tag).
			Return(wantDeleted, nil)

		blobs := mockstore.NewBlobs(t)
		for _, id := range wantDeleted {
			blobs.EXPECT().
				DeleteBlob(id).
				Return(nil)
		}

		svc := New(blobs, repo)
		err := svc.DeleteTag(tag)
		AssertNoError(t, err)
	})
}
