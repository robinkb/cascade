package server_test

import (
	"net/http"
	"strconv"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/robinkb/cascade-registry/testing/mock"
)

func TestStatManifests(t *testing.T) {
	name := RandomName()
	manifest, digest := RandomManifest()
	length := len(manifest.Bytes())
	tag := RandomVersion()

	t.Run("Stat existing manifest returns 200 with correct size in Content-Length header", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			StatManifest(name, digest.String()).
			Return(&cascade.FileInfo{Size: int64(length)}, nil)

		client := NewTestClientWithServer(t, service)

		resp := client.CheckManifestByDigest(name, digest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentLength, strconv.Itoa(len(manifest.Bytes())))
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Stat existing manifest by tag returns 200 with correct size in Content-Length header", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			GetTag(name, tag).
			Return(digest.String(), nil)
		service.EXPECT().
			StatManifest(name, digest.String()).
			Return(&cascade.FileInfo{Size: int64(length)}, nil)

		client := NewTestClientWithServer(t, service)

		resp := client.CheckManifestByTag(name, tag)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentLength, strconv.Itoa(len(manifest.Bytes())))
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Stat non-existent manifest returns 404 and ErrManifestUknown", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			StatManifest(name, digest.String()).
			Return(nil, cascade.ErrManifestUnknown)

		client := NewTestClientWithServer(t, service)

		resp := client.CheckManifestByDigest(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
	})

	t.Run("Stat non-existent manifest by tag returns 404 and ErrManifestUknown", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			GetTag(name, tag).
			Return("", cascade.ErrManifestUnknown)

		client := NewTestClientWithServer(t, service)

		resp := client.CheckManifestByTag(name, tag)

		AssertResponseCode(t, resp, http.StatusNotFound)
	})
}

func TestGetManifests(t *testing.T) {
	name := RandomName()
	manifest, digest := RandomManifest()
	tag := RandomVersion()

	t.Run("Retrieving an existing manifest returns 200", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			GetManifest(name, digest.String()).
			Return(cascade.NewManifest(manifest.Bytes()))

		client := NewTestClientWithServer(t, service)

		resp := client.GetManifestByDigest(name, digest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentType, v1.MediaTypeImageManifest)
		AssertResponseBodyEquals(t, resp, manifest.Bytes())
	})

	t.Run("Retrieving a manifest by tag returns 200", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			GetTag(name, tag).
			Return(digest.String(), nil)
		service.EXPECT().
			GetManifest(name, digest.String()).
			Return(cascade.NewManifest(manifest.Bytes()))

		client := NewTestClientWithServer(t, service)

		resp := client.GetManifestByTag(name, tag)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseBodyEquals(t, resp, manifest.Bytes())
	})

	t.Run("Retrieving a non-existent manifest returns status 404 and ErrManifestUnknown", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			GetManifest(name, digest.String()).
			Return(nil, cascade.ErrManifestUnknown)

		client := NewTestClientWithServer(t, service)

		resp := client.GetManifestByDigest(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
		AssertResponseBodyContainsError(t, resp, cascade.ErrManifestUnknown)
	})
}

func TestPutManifest(t *testing.T) {
	name := RandomName()
	manifest, digest := RandomManifest()
	tag := RandomVersion()

	t.Run("Uploading a manifest by digest returns code 201", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			PutManifest(name, digest.String(), manifest.Bytes()).
			Return("", nil)

		client := NewTestClientWithServer(t, service)

		resp := client.PutManifest(name, digest.String(), manifest)

		AssertResponseCode(t, resp, http.StatusCreated)
		AssertResponseHeaderSet(t, resp, server.HeaderLocation)
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Uploading a manifest by tag returns code 201", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			PutTag(name, tag, digest.String()).
			Return(nil)
		service.EXPECT().
			PutManifest(name, digest.String(), manifest.Bytes()).
			Return("", nil)

		client := NewTestClientWithServer(t, service)

		resp := client.PutManifest(name, tag, manifest)

		AssertResponseCode(t, resp, http.StatusCreated)
		AssertResponseHeaderSet(t, resp, server.HeaderLocation)
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Uploading an invalid manifest returns 400 and ErrManifestInvalid", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			PutManifest(name, digest.String(), manifest.Bytes()).
			Return("", cascade.ErrManifestInvalid)

		client := NewTestClientWithServer(t, service)

		resp := client.PutManifest(name, tag, manifest)

		AssertResponseCode(t, resp, http.StatusBadRequest)
		AssertResponseBodyContainsError(t, resp, cascade.ErrManifestInvalid)
	})

	t.Run("Uploading a manifest with subject returns with OCI-Subject header", func(t *testing.T) {
		subjectManifest, subjectDigest := RandomManifest()
		referrerManifest, referrerDigest := RandomManifestWithSubject(subjectManifest)

		service := mock.NewRegistryService(t)
		service.EXPECT().
			PutManifest(name, referrerDigest.String(), referrerManifest.Bytes()).
			Return(subjectDigest, nil)

		client := NewTestClientWithServer(t, service)

		resp := client.PutManifest(name, referrerDigest.String(), referrerManifest)

		AssertResponseCode(t, resp, http.StatusCreated)
		AssertResponseHeader(t, resp, server.HeaderOCISubject, subjectDigest.String())
	})
}

func TestDeleteManifest(t *testing.T) {
	name := RandomName()
	digest := RandomDigest()
	tag := RandomVersion()

	t.Run("Deleting a manifest returns code 202", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			DeleteManifest(name, digest.String()).
			Return(nil)

		client := NewTestClientWithServer(t, service)

		resp := client.DeleteManifest(name, digest)

		AssertResponseCode(t, resp, http.StatusAccepted)
	})

	t.Run("Deleting a manifest by tag returns 202", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			DeleteTag(name, tag).
			Return(nil)

		client := NewTestClientWithServer(t, service)

		resp := client.DeleteTag(name, tag)

		AssertResponseCode(t, resp, http.StatusAccepted)
	})

	t.Run("Deleting an unknown manifest returns 404", func(t *testing.T) {
		service := mock.NewRegistryService(t)
		service.EXPECT().
			DeleteManifest(name, digest.String()).
			Return(cascade.ErrManifestUnknown)

		client := NewTestClientWithServer(t, service)

		resp := client.DeleteManifest(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
		AssertResponseBodyContainsError(t, resp, cascade.ErrManifestUnknown)
	})
}

func TestManifestsOthers(t *testing.T) {
	t.Run("Other methods are not allowed", func(t *testing.T) {
		client := NewTestClientWithServer(t, nil)

		resp := client.Do(
			http.MethodTrace,
			"/v2/library/fedora/manifests/1.0.0",
			nil, nil,
		)

		AssertResponseCode(t, resp, http.StatusMethodNotAllowed)
	})
}
