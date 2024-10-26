package cascade

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestStatManifest(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("returns FileInfo on known manifest", func(t *testing.T) {
		repository := "asflkn/waekln"
		content := randomContents(32)
		digest := fmt.Sprintf("sha256:%x", sha256.Sum256(content))

		err := service.PutManifest(repository, digest, content)
		assertNoError(t, err)

		info, err := service.StatManifest(repository, digest)
		assertNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("got size of %d, expected %d", got, want)
		}
		assertNoError(t, err)
	})

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.StatManifest("do/not", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		assertErrorIs(t, err, ErrManifestUnknown)
	})
}

func TestGetManifest(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.GetManifest("i/do/not/exist", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		assertErrorIs(t, err, ErrManifestUnknown)
	})
}
