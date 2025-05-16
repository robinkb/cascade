package server_test

import (
	"bytes"
	"net/http"
	"strconv"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/robinkb/cascade-registry/testing/mock"
)

func TestStatBlob(t *testing.T) {
	name, digest := RandomName(), RandomDigest()

	t.Run("known blob returns 200", func(t *testing.T) {
		var size int64 = 42

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			StatBlob(name, digest.String()).
			Return(&cascade.FileInfo{Size: size}, nil)

		client := NewTestClientWithRepository(t, name, repository)

		resp := client.CheckBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentLength, strconv.FormatInt(size, 10))
		AssertResponseBodyEquals(t, resp, nil)
	})

	t.Run("unknown blob returns 404", func(t *testing.T) {
		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			StatBlob(name, digest.String()).
			Return(nil, cascade.ErrBlobUnknown)

		client := NewTestClientWithRepository(t, name, repository)

		resp := client.CheckBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
	})
}

func TestGetBlob(t *testing.T) {
	name := RandomName()
	digest, content := RandomBlob(32)

	t.Run("Get blob returns 200", func(t *testing.T) {
		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			GetBlob(name, digest.String()).
			Return(bytes.NewBuffer(content), nil)

		client := NewTestClientWithRepository(t, name, repository)

		resp := client.GetBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseBodyEquals(t, resp, content)
	})

	t.Run("returns 404 on unknown blob", func(t *testing.T) {
		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			GetBlob(name, digest.String()).
			Return(nil, cascade.ErrBlobUnknown)

		client := NewTestClientWithRepository(t, name, repository)

		resp := client.GetBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
		AssertResponseBodyContainsError(t, resp, cascade.ErrBlobUnknown)
	})

}

func TestDeleteBlob(t *testing.T) {
	name, digest := RandomName(), RandomDigest()

	t.Run("Delete blob returns 202", func(t *testing.T) {
		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			DeleteBlob(name, digest.String()).
			Return(nil)

		client := NewTestClientWithRepository(t, name, repository)

		resp := client.DeleteBlob(name, digest)

		AssertResponseCode(t, resp, http.StatusAccepted)
	})
}

func TestBlobsOthers(t *testing.T) {
	t.Run("other methods return 405", func(t *testing.T) {
		client := NewTestClientWithServer(t, nil)

		resp := client.Do(http.MethodConnect, "/v2/library/fedora/blobs/123", nil, nil)

		AssertResponseCode(t, resp, http.StatusMethodNotAllowed)
	})
}
