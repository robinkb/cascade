package server_test

import (
	"errors"
	"net/http"
	"strconv"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/robinkb/cascade-registry/testing/mock"
)

func TestStatManifests(t *testing.T) {
	name := RandomName()
	digest, _, content := RandomManifest()
	length := len(content)
	tag := RandomVersion()

	t.Run("Stat existing manifest returns 200 with correct size in Content-Length header", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			StatManifest(name, digest.String()).
			Return(&store.BlobInfo{Size: int64(length)}, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckManifestByDigest(name, digest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentLength, strconv.Itoa(len(content)))
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Stat existing manifest by tag returns 200 with correct size in Content-Length header", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			GetTag(name, tag).
			Return(digest.String(), nil)
		repo.EXPECT().
			StatManifest(name, digest.String()).
			Return(&store.BlobInfo{Size: int64(length)}, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckManifestByTag(name, tag)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentLength, strconv.Itoa(len(content)))
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Stat non-existent manifest returns 404 and ErrManifestUknown", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			StatManifest(name, digest.String()).
			Return(nil, repository.ErrManifestUnknown)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckManifestByDigest(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
	})

	t.Run("Stat non-existent manifest by tag returns 404 and ErrManifestUknown", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			GetTag(name, tag).
			Return("", repository.ErrManifestUnknown)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckManifestByTag(name, tag)

		AssertResponseCode(t, resp, http.StatusNotFound)
	})
}

func TestGetManifests(t *testing.T) {
	name := RandomName()
	digest, manifest, content := RandomManifest()
	meta := &store.ManifestMetadata{
		MediaType: manifest.MediaType,
	}
	tag := RandomVersion()

	t.Run("Retrieving an existing manifest returns 200", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			GetManifest(name, digest.String()).
			Return(meta, content, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.GetManifestByDigest(name, digest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentType, v1.MediaTypeImageManifest)
		AssertResponseBodyEquals(t, resp, content)
	})

	t.Run("Retrieving a manifest by tag returns 200", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			GetTag(name, tag).
			Return(digest.String(), nil)
		repo.EXPECT().
			GetManifest(name, digest.String()).
			Return(meta, content, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.GetManifestByTag(name, tag)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseBodyEquals(t, resp, content)
	})

	t.Run("Retrieving a non-existent manifest returns status 404 and ErrManifestUnknown", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			GetManifest(name, digest.String()).
			Return(nil, nil, repository.ErrManifestUnknown)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.GetManifestByDigest(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
		AssertResponseBodyContainsError(t, resp, repository.ErrManifestUnknown)
	})
}

func TestPutManifest(t *testing.T) {
	name := RandomName()
	digest, manifest, content := RandomManifest()
	tag := RandomVersion()

	t.Run("Uploading a manifest by digest returns code 201", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			PutManifest(name, digest.String(), content).
			Return("", nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.PutManifest(name, digest.String(), content)

		AssertResponseCode(t, resp, http.StatusCreated)
		AssertResponseHeaderSet(t, resp, server.HeaderLocation)
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Uploading a manifest by tag returns code 201", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			PutTag(name, tag, digest.String()).
			Return(nil)
		repo.EXPECT().
			PutManifest(name, digest.String(), content).
			Return("", nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.PutManifest(name, tag, content)

		AssertResponseCode(t, resp, http.StatusCreated)
		AssertResponseHeaderSet(t, resp, server.HeaderLocation)
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Uploading a manifest with subject returns OCI-Subject header set to subject digest", func(t *testing.T) {
		subjectDigest, subjectManifest := digest, manifest
		digest, _, content := RandomManifestWithSubject(subjectDigest, subjectManifest)

		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			PutManifest(name, digest.String(), content).
			Return(subjectDigest, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.PutManifest(name, digest.String(), content)

		AssertResponseCode(t, resp, http.StatusCreated)
		AssertResponseHeaderSet(t, resp, server.HeaderLocation)
		AssertResponseHeader(t, resp, server.HeaderOCISubject, subjectDigest.String())
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("Uploading an invalid manifest returns 400 and ErrManifestInvalid", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			PutManifest(name, digest.String(), content).
			Return("", repository.ErrManifestInvalid)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.PutManifest(name, tag, content)

		AssertResponseCode(t, resp, http.StatusBadRequest)
		AssertResponseBodyContainsError(t, resp, repository.ErrManifestInvalid)
	})

	t.Run("Other service error returns 500", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			PutManifest(name, digest.String(), content).
			Return("", errors.New("unknown"))

		client := NewTestClientForRepository(t, name, repo)

		resp := client.PutManifest(name, digest.String(), content)

		AssertResponseCode(t, resp, http.StatusInternalServerError)
	})
}

func TestDeleteManifest(t *testing.T) {
	name := RandomName()
	digest := RandomDigest()
	tag := RandomVersion()

	t.Run("Deleting a manifest returns code 202", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			DeleteManifest(name, digest.String()).
			Return(nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.DeleteManifest(name, digest)

		AssertResponseCode(t, resp, http.StatusAccepted)
	})

	t.Run("Deleting a manifest by tag returns 202", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			DeleteTag(name, tag).
			Return(nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.DeleteTag(name, tag)

		AssertResponseCode(t, resp, http.StatusAccepted)
	})

	t.Run("Deleting an unknown manifest returns 404", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			DeleteManifest(name, digest.String()).
			Return(repository.ErrManifestUnknown)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.DeleteManifest(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
		AssertResponseBodyContainsError(t, resp, repository.ErrManifestUnknown)
	})
}

func TestManifestsOthers(t *testing.T) {
	t.Run("Other methods are not allowed", func(t *testing.T) {
		client := NewTestClientForHandler(t, server.New(nil))

		resp := client.Do(
			http.MethodTrace,
			"/v2/library/fedora/manifests/1.0.0",
			nil, nil,
		)

		AssertResponseCode(t, resp, http.StatusMethodNotAllowed)
	})
}
