package cascade_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/robinkb/cascade-registry"
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
		assertErrorIs(t, err, cascade.ErrManifestUnknown)
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
		assertErrorIs(t, err, cascade.ErrManifestUnknown)
	})
}

func TestListTag(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Listing tags returns all in lexical order", func(t *testing.T) {
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

		got, err := service.ListTags(name, -1, "")
		assertNoError(t, err)

		want := tags

		if !slices.Equal(got, want) {
			t.Fatalf("expected to see all tags; got %q, want %q", got, want)
		}
	})

	t.Run("Listing tags with a count limit returns fewer tags", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tags := randomTags(20)
		count := 5

		for _, tag := range tags {
			err := service.PutTag(name, tag, digest.String())
			assertNoError(t, err)
		}

		got, err := service.ListTags(name, count, "")
		assertNoError(t, err)

		want := tags[0:count]

		if !slices.Equal(got, want) {
			t.Fatalf("Unexpected subset of tags; got %q, want %q", got, want)
		}
	})

	t.Run("Listing tags with a count of 0 must return an empty list", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tags := randomTags(5)
		count := 0

		for _, tag := range tags {
			err := service.PutTag(name, tag, digest.String())
			assertNoError(t, err)
		}

		got, err := service.ListTags(name, count, "")
		assertNoError(t, err)

		if len(got) != count {
			t.Fatal("List tags with count of 0 returned a non-empty list")
		}
	})

	t.Run("Listing tags with a count greater than the number of tags returns all tags", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tags := randomTags(5)
		count := 6

		for _, tag := range tags {
			err := service.PutTag(name, tag, digest.String())
			assertNoError(t, err)
		}

		got, err := service.ListTags(name, count, "")
		assertNoError(t, err)

		if !slices.Equal(got, tags) {
			t.Fatalf("Returned tags is not equal to actual tags; got %q, want %q", got, tags)
		}
	})

	t.Run("Listing tags from a certain tag only returns tags after that tag", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tags := randomTags(10)
		count := 3
		last := 4

		for _, tag := range tags {
			err := service.PutTag(name, tag, digest.String())
			assertNoError(t, err)
		}

		got, err := service.ListTags(name, count, tags[last])
		assertNoError(t, err)

		want := tags[last+1 : last+1+count]

		if !slices.Equal(got, want) {
			t.Fatalf("Unexpected subset of tags; last %q, got %q, want %q", tags[last], got, want)
		}
	})

	t.Run("When listing tags from a certain tag, the count parameter may be -1 to return all tags", func(t *testing.T) {
		name, digest, _ := randomManifest()
		tags := randomTags(10)
		count := -1
		last := 4

		for _, tag := range tags {
			err := service.PutTag(name, tag, digest.String())
			assertNoError(t, err)
		}

		got, err := service.ListTags(name, count, tags[last])
		assertNoError(t, err)

		want := tags[last+1:]

		if !slices.Equal(got, want) {
			t.Fatalf("Unexpected subset of tags; last %q, got %q, want %q", tags[last], got, want)
		}
	})
}

func randomTags(count int) (tags []string) {
	for range count {
		tags = append(tags, fmt.Sprintf("v%d.%d.%d",
			rand.IntN(100), rand.IntN(100), rand.IntN(100),
		))
	}

	slices.Sort(tags)

	return
}
