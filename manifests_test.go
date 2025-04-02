package cascade_test

import (
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestStatManifest(t *testing.T) {
	service, metadata, blobs := newTestRegistry()

	name := RandomName()
	digest, manifest := RandomManifest()

	path := digest.String()
	blobs.Put(path, manifest.Bytes())
	metadata.PutManifest(name, digest, path, nil)

	t.Run("Returns FileInfo with expected size on known manifest", func(t *testing.T) {
		info, err := service.StatManifest(name, digest.String())
		AssertNoError(t, err)

		got := info.Size
		want := int64(len(manifest.Bytes()))

		if got != want {
			t.Errorf("got size of %d, expected %d", got, want)
		}
		AssertNoError(t, err)
	})

	t.Run("Returns ErrManifestUnkonwn on known manifest in other repository", func(t *testing.T) {
		_, err := service.StatManifest("unknown/repository", digest.String())
		AssertErrorIs(t, err, cascade.ErrManifestUnknown)
	})

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.StatManifest(name, "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		AssertErrorIs(t, err, cascade.ErrManifestUnknown)
	})

	t.Run("returns ErrManifestUnknown on invalid digest", func(t *testing.T) {
		_, err := service.StatManifest(name, "sha256:i-am-not-valid-lol")
		AssertErrorIs(t, err, cascade.ErrManifestUnknown)
	})
}

func TestGetManifest(t *testing.T) {
	service, metadata, blobs := newTestRegistry()

	name := RandomName()
	manifest, _ := json.Marshal(v1.Manifest{MediaType: v1.MediaTypeImageLayer})
	digest := digest.FromBytes(manifest)

	path := digest.String()
	blobs.Put(path, manifest)
	metadata.PutManifest(name, digest, path, nil)

	t.Run("Retrieve an existing manifest", func(t *testing.T) {
		got, err := service.GetManifest(name, digest.String())
		AssertNoError(t, err)
		AssertSlicesEqual(t, got.Bytes(), manifest)
	})

	t.Run("returns ErrManifestUnknown on unknown manifest", func(t *testing.T) {
		_, err := service.GetManifest("i/do/not/exist", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		AssertErrorIs(t, err, cascade.ErrManifestUnknown)
	})
}

func TestPutManifest(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Put and retrieve a manifest", func(t *testing.T) {
		name := RandomName()
		digest, manifest := RandomManifest()

		_, err := service.PutManifest(name, digest.String(), manifest.Bytes())
		AssertNoError(t, err)

		got, err := service.GetManifest(name, digest.String())
		AssertNoError(t, err)
		AssertSlicesEqual(t, got.Bytes(), manifest.Bytes())
	})

	t.Run("Putting a manifest with invalid content returns ErrManifestInvalid", func(t *testing.T) {
		name := RandomName()
		digest, content := RandomBlob(32)

		_, err := service.PutManifest(name, digest.String(), content)
		AssertErrorIs(t, err, cascade.ErrManifestInvalid)
	})

	t.Run("Putting a manifest with subject returns the subject's hash", func(t *testing.T) {
		name := RandomName()
		subjectDigest, subject := RandomManifest()
		referrer, referrerDigest := RandomManifestWithSubject(subject)

		subjectDescriptor, err := service.PutManifest(name, referrerDigest.String(), referrer.Bytes())
		AssertNoError(t, err)
		if subjectDescriptor.Digest.String() != subjectDigest.String() {
			t.Errorf("invalid subject digest; got %s, want %s", subjectDescriptor.Digest, subjectDigest)
		}
	})
}

func TestDeleteManifest(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Delete manifest and make sure it cannot be retrieved", func(t *testing.T) {
		name := RandomName()
		digest, manifest := RandomManifest()

		_, err := service.PutManifest(name, digest.String(), manifest.Bytes())
		RequireNoError(t, err)

		_, err = service.StatManifest(name, digest.String())
		RequireNoError(t, err)

		err = service.DeleteManifest(name, digest.String())
		AssertNoError(t, err)

		_, err = service.StatManifest(name, digest.String())
		AssertErrorIs(t, err, cascade.ErrManifestUnknown)
	})

	t.Run("Deleting an unknown manifest returns ErrManifestUknown", func(t *testing.T) {
		err := service.DeleteManifest("does/not/exist", "123")
		AssertErrorIs(t, err, cascade.ErrManifestUnknown)
	})
}
