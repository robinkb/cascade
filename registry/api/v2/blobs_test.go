package v2

import (
	"bytes"
	"net/http"
	"strconv"
	"testing"

	"github.com/robinkb/cascade/registry/repository"
	"github.com/robinkb/cascade/registry/store"
	. "github.com/robinkb/cascade/testing"
	testclient "github.com/robinkb/cascade/testing/client"
	"github.com/robinkb/cascade/testing/mock"
)

func TestStatBlob(t *testing.T) {
	name, digest := RandomName(), RandomDigest()

	t.Run("known blob returns 200", func(t *testing.T) {
		var size int64 = 42

		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			StatBlob(name, digest.String()).
			Return(&store.BlobInfo{Size: size}, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, HeaderContentLength, strconv.FormatInt(size, 10))
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("unknown blob returns 404", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			StatBlob(name, digest.String()).
			Return(nil, repository.ErrBlobUnknown)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
	})

	t.Run("unknown repository returns 404", func(t *testing.T) {
		registry := mock.NewRegistryService(t)
		registry.EXPECT().
			GetRepository(name).
			Return(nil, repository.ErrNameUnknown)

		client := testclient.NewTestClientForHandler(t, New(registry))
		resp := client.CheckBlob(name, digest)
		AssertResponseCode(t, resp, http.StatusNotFound)
	})
}

func TestGetBlob(t *testing.T) {
	name := RandomName()
	digest, content := RandomBlob(32)

	t.Run("Get blob returns 200", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			GetBlob(name, digest.String()).
			Return(bytes.NewBuffer(content), nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.GetBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseBodyEquals(t, resp, content)
	})

	t.Run("returns 404 on unknown blob", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			GetBlob(name, digest.String()).
			Return(nil, repository.ErrBlobUnknown)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.GetBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
		AssertResponseBodyContainsError(t, resp, repository.ErrBlobUnknown)
	})

	t.Run("unknown repository returns 404", func(t *testing.T) {
		registry := mock.NewRegistryService(t)
		registry.EXPECT().
			GetRepository(name).
			Return(nil, repository.ErrNameUnknown)

		client := testclient.NewTestClientForHandler(t, New(registry))
		resp := client.GetBlob(name, digest)
		AssertResponseCode(t, resp, http.StatusNotFound)
	})
}

func TestDeleteBlob(t *testing.T) {
	name, digest := RandomName(), RandomDigest()

	t.Run("Delete blob returns 202", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			DeleteBlob(name, digest.String()).
			Return(nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.DeleteBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusAccepted)
	})

	t.Run("unknown repository returns 404", func(t *testing.T) {
		registry := mock.NewRegistryService(t)
		registry.EXPECT().
			GetRepository(name).
			Return(nil, repository.ErrNameUnknown)

		client := testclient.NewTestClientForHandler(t, New(registry))
		resp := client.DeleteBlob(name, digest)
		AssertResponseCode(t, resp, http.StatusNotFound)
	})
}

func TestBlobsOthers(t *testing.T) {
	t.Run("other methods return 405", func(t *testing.T) {
		client := testclient.NewTestClientForHandler(t, New(nil))

		resp := client.Do(http.MethodConnect, "/v2/library/fedora/blobs/123", nil, nil)

		AssertResponseCode(t, resp, http.StatusMethodNotAllowed)
	})
}
