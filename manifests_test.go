package cascade_test

import (
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
)

func TestStatManifest(t *testing.T) {
	service, metadata, blobs := newTestRegistry()

	name := randomName()
	manifest, _ := json.Marshal(v1.Manifest{MediaType: v1.MediaTypeImageLayer})
	digest := digest.FromBytes(manifest)

	path := digest.String()
	blobs.Put(path, manifest)
	metadata.PutManifest(name, digest, path)

	t.Run("Returns FileInfo with expected size on known manifest", func(t *testing.T) {
		info, err := service.StatManifest(name, digest.String())
		assertNoError(t, err)

		got := info.Size
		want := int64(len(manifest))

		if got != want {
			t.Errorf("got size of %d, expected %d", got, want)
		}
		assertNoError(t, err)
	})

	t.Run("Returns ErrManifestUnkonwn on known manifest in other repository", func(t *testing.T) {
		_, err := service.StatManifest("unknown/repository", digest.String())
		assertErrorIs(t, err, cascade.ErrManifestUnknown)
	})

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.StatManifest(name, "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		assertErrorIs(t, err, cascade.ErrManifestUnknown)
	})

	t.Run("returns ErrManifestUnknown on invalid digest", func(t *testing.T) {
		_, err := service.StatManifest(name, "sha256:i-am-not-valid-lol")
		assertErrorIs(t, err, cascade.ErrManifestUnknown)
	})
}

func TestGetManifest(t *testing.T) {
	service, metadata, blobs := newTestRegistry()

	name := randomName()
	manifest, _ := json.Marshal(v1.Manifest{MediaType: v1.MediaTypeImageLayer})
	digest := digest.FromBytes(manifest)

	path := digest.String()
	blobs.Put(path, manifest)
	metadata.PutManifest(name, digest, path)

	t.Run("Retrieve an existing manifest", func(t *testing.T) {
		got, err := service.GetManifest(name, digest.String())
		assertNoError(t, err)
		assertContent(t, got.Bytes(), manifest)
	})

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.GetManifest("i/do/not/exist", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		assertErrorIs(t, err, cascade.ErrManifestUnknown)
	})
}

func TestPutManifest(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Put and retrieve a manifest", func(t *testing.T) {
		name, digest, manifest := randomManifest()

		err := service.PutManifest(name, digest.String(), manifest)
		assertNoError(t, err)

		got, err := service.GetManifest(name, digest.String())
		assertNoError(t, err)
		assertContent(t, got.Bytes(), manifest)
	})

	t.Run("Putting a manifest with invalid content returns ErrManifestInvalid", func(t *testing.T) {
		name, digest, content := randomBlob(32)

		err := service.PutManifest(name, digest.String(), content)
		assertErrorIs(t, err, cascade.ErrManifestInvalid)
	})
}

func TestDeleteManifest(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Delete manifest and make sure it cannot be retrieved", func(t *testing.T) {
		name, digest, manifest := randomManifest()

		err := service.PutManifest(name, digest.String(), manifest)
		assertNoError(t, err)

		_, err = service.StatManifest(name, digest.String())
		assertNoError(t, err)

		err = service.DeleteManifest(name, digest.String())
		assertNoError(t, err)

		_, err = service.StatManifest(name, digest.String())
		assertErrorIs(t, err, cascade.ErrManifestUnknown)
	})

	t.Run("Deleting an unknown manifest returns ErrManifestUknown", func(t *testing.T) {
		err := service.DeleteManifest("does/not/exist", "123")
		assertErrorIs(t, err, cascade.ErrManifestUnknown)
	})
}

func randomManifest() (string, digest.Digest, []byte) {
	name := randomName()
	manifest, _ := json.Marshal(v1.Manifest{
		MediaType: v1.MediaTypeImageLayer,
	})
	digest := digest.FromBytes(manifest)

	return name, digest, manifest
}
