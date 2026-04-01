package repository

import (
	"math/rand/v2"
	"testing"

	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	mockstore "github.com/robinkb/cascade/testing/mock/store"
)

func TestStatBlob(t *testing.T) {
	t.Run("returns blob info for digest", func(t *testing.T) {
		id := RandomDigest()
		info := &store.BlobInfo{
			Name: RandomName(),
			Size: rand.Int64(),
		}
		blobs := mockstore.NewBlobs(t)
		blobs.EXPECT().
			StatBlob(id).
			Return(info, nil)
		repo := mockstore.NewRepository(t)
		repo.EXPECT().
			GetBlob(id).
			Return(nil)

		r := NewRepositoryService(blobs, repo)
		got, err := r.StatBlob(id.String())
		AssertNoError(t, err)
		AssertDeepEqual(t, got, info)
	})
}
