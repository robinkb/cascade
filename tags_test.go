package cascade

import (
	"testing"

	"github.com/robinkb/cascade-registry/paths"
)

func TestGetTag(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("Manifest digest is retrievable by tag", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tag := "v1.2.3"

		store.Set(paths.MetaStore.TagLink(name, tag), []byte(digest))

		got, err := service.GetTag(name, tag)
		assertNoError(t, err)

		if got != digest.String() {
			t.Errorf("wrong digest retrieved; got %s, want %s", got, digest.String())
		}
	})

	t.Run("Unknown tag returns ErrManifestUnknown", func(t *testing.T) {
		_, err := service.GetTag("non/existant", "v1.2.3")
		assertErrorIs(t, err, ErrManifestUnknown)
	})
}

func TestPutTag(t *testing.T) {
	t.Run("Tag creates a link to the manifest digest", func(t *testing.T) {
		name, digest, manifest := randomManifest()
		tag := "v0.5.1"

		store := NewInMemoryStore()
		service := NewRegistryService(store)

		err := service.PutManifest(name, digest.String(), manifest)
		assertNoError(t, err)

		err = service.PutTag(name, tag, digest.String())
		assertNoError(t, err)

		gotDigest, err := service.GetTag(name, tag)
		assertNoError(t, err)

		gotManifest, err := service.GetManifest(name, gotDigest)
		assertNoError(t, err)

		assertContent(t, gotManifest.Bytes(), manifest)
	})
}
