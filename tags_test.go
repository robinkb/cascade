package cascade

import (
	"slices"
	"testing"
)

func TestGetTag(t *testing.T) {
	service, metadata, _ := newTestRegistry()

	t.Run("Manifest digest is retrievable by tag", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tag := "v1.2.3"

		metadata.PutTag(name, tag, digest.String())

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
	service, _, _ := newTestRegistry()

	t.Run("Tag creates a link to the manifest digest", func(t *testing.T) {
		name, digest, manifest := randomManifest()
		tag := "v0.5.1"

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

func TestDeleteTag(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Deleted tag is not retrievable", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tag := "v0.5.1"

		err := service.PutTag(name, tag, digest.String())
		assertNoError(t, err)

		_, err = service.GetTag(name, tag)
		assertNoError(t, err)

		err = service.DeleteTag(name, tag)
		assertNoError(t, err)

		_, err = service.GetTag(name, tag)
		assertErrorIs(t, err, ErrManifestUnknown)
	})
}

func TestListTag(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Listing tags returns all", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tags := []string{
			"v1.0.0",
			"v1.1.0",
			"v1.1.1",
			"v1.1.2",
		}

		for _, tag := range tags {
			err := service.PutTag(name, tag, string(digest))
			assertNoError(t, err)
		}

		got, err := service.ListTags(name)
		assertNoError(t, err)

		want := tags

		if !slices.Equal(got, want) {
			t.Fatalf("expected to see all tags; got %q, want %q", got, want)
		}
	})
}
