package repository

import (
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	mockstore "github.com/robinkb/cascade/testing/mock/store"
)

func TestPutManifest(t *testing.T) {
	t.Run("processes an image manifest", func(t *testing.T) {
		id, data := loadTestimageManifest(t)
		wantMetadata := store.Manifest{
			Annotations: map[string]string{
				"org.opencontainers.image.base.digest": "",
				"org.opencontainers.image.base.name":   "",
				"org.opencontainers.image.created":     "2026-04-01T07:53:33.547482297Z",
			},
			ArtifactType: "application/vnd.oci.image.config.v1+json",
			MediaType:    "application/vnd.oci.image.manifest.v1+json",
			Size:         int64(len(data)),
		}
		wantReferences := store.References{
			Config: "sha256:01fd43ca5fd99acc99a708d48d94b2b6e3deb81e8ee1c76202a612cd3fdd4eea",
			Layers: []digest.Digest{
				"sha256:51a96c573d269b0aeaf05ac2bbf812a3a2a8d9a6a6d6cd51dcee27f2662ecc61",
			},
		}

		blobs := mockstore.NewBlobs(t)
		blobs.EXPECT().
			PutBlob(id, data).
			Return(nil)

		repo := mockstore.NewRepository(t)
		repo.EXPECT().
			PutManifest(id, wantMetadata, wantReferences).
			Return(nil)

		svc := New(blobs, repo)
		subj, err := svc.PutManifest(id.String(), data)
		AssertEqual(t, subj, "")
		AssertNoError(t, err)
	})

	t.Run("processes a manifest with subject", func(t *testing.T) {
		subject := NewImageManifestBuilder(t).Build()
		referrer := NewImageManifestBuilder(t).WithSubject(subject.Manifest).Build()

		blobs := mockstore.NewBlobs(t)
		blobs.EXPECT().
			PutBlob(referrer.Digest, referrer.Bytes).
			Return(nil)

		repo := mockstore.NewRepository(t)
		repo.EXPECT().
			PutManifest(referrer.Digest, referrer.Metadata(), referrer.References()).
			Return(nil)

		svc := New(blobs, repo)
		id, err := svc.PutManifest(referrer.Digest.String(), referrer.Bytes)
		AssertNoError(t, err)
		AssertEqual(t, id, subject.Digest)
	})

	t.Run("processes an image index", func(t *testing.T) {
		id, data := loadTestimageIndex(t)
		wantMedata := store.Manifest{
			MediaType: "application/vnd.oci.image.index.v1+json",
			Size:      int64(len(data)),
		}
		wantReferences := store.References{
			Manifests: []digest.Digest{
				"sha256:845418faa147747485688164beb1120ee9685f41725d14f69e46c649aa4ee475",
				"sha256:ca40dc79f7809debd4e88b0ff6a57b9b05b19e36979e78b65b8d9e239bdf935e",
				"sha256:76e93bef097cd26873d87c93b67ef3be1957a2ffbd96385dbb613909e78bbd3a",
				"sha256:8a736c552ebb686ee9f902e4e0857b16985157f0e0057bfc33c4f08c3c7971b1",
			},
		}

		blobs := mockstore.NewBlobs(t)
		blobs.EXPECT().
			PutBlob(id, data).
			Return(nil)

		repo := mockstore.NewRepository(t)
		repo.EXPECT().
			PutManifest(id, wantMedata, wantReferences).
			Return(nil)

		svc := New(blobs, repo)
		subj, err := svc.PutManifest(id.String(), data)
		AssertNoError(t, err)
		AssertEqual(t, subj, "")
	})
}

func TestDeleteManifest(t *testing.T) {
	t.Run("deletes garbage collected blobs", func(t *testing.T) {
		manifest := NewImageManifestBuilder(t).WithLayers(5).Build()

		repo := mockstore.NewRepository(t)
		repo.EXPECT().
			GetManifest(manifest.Digest).
			Return(store.Manifest{}, nil)
		repo.EXPECT().
			DeleteManifest(manifest.Digest).
			Return(manifest.LayersAsDigests(), nil)

		blobs := mockstore.NewBlobs(t)
		for _, layer := range manifest.Manifest.Layers {
			blobs.EXPECT().
				DeleteBlob(layer.Digest).
				Return(nil)
		}

		svc := New(blobs, repo)
		err := svc.DeleteManifest(manifest.Digest.String())
		AssertNoError(t, err)
	})
}

func loadTestimageManifest(t *testing.T) (id digest.Digest, data []byte) {
	manifest := new(v1.Manifest)
	f, err := os.Open("testdata/image_manifest.json")
	AssertNoError(t, err).Require()

	data, err = io.ReadAll(f)
	AssertNoError(t, err).Require()
	err = f.Close()
	AssertNoError(t, err).Require()

	id = digest.FromBytes(data)
	err = json.Unmarshal(data, manifest)
	AssertNoError(t, err).Require()
	return
}

func loadTestimageIndex(t *testing.T) (id digest.Digest, data []byte) {
	index := new(v1.Index)
	f, err := os.Open("testdata/image_index.json")
	AssertNoError(t, err).Require()

	data, err = io.ReadAll(f)
	AssertNoError(t, err).Require()
	err = f.Close()
	AssertNoError(t, err).Require()

	id = digest.FromBytes(data)
	err = json.Unmarshal(data, index)
	AssertNoError(t, err).Require()
	return
}
