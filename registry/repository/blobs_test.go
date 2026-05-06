package repository

import (
	"math/rand/v2"
	"testing"

	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing"
	"github.com/robinkb/cascade/testing/store/mock"
)

func TestStatBlob(t *testing.T) {
	t.Run("returns blob info for digest", func(t *testing.T) {
		id := RandomDigest()
		info := &store.BlobInfo{
			Name: RandomName(),
			Size: rand.Int64(),
		}
		blobs := mock.NewBlobs(t)
		blobs.EXPECT().
			StatBlob(id).
			Return(info, nil)
		repo := mock.NewRepository(t)
		repo.EXPECT().
			GetLink(id).
			Return(nil)

		r := New(blobs, repo)
		got, err := r.StatBlob(id.String())
		AssertNoError(t, err)
		AssertDeepEqual(t, got, info)
	})
}
