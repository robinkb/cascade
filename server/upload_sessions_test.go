package server_test

import (
	"net/http"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/robinkb/cascade-registry/testing/mock"
)

func TestBlobUploadSession(t *testing.T) {
	name := RandomName()
	sessionID, _ := uuid.NewV7()
	location := newLocation(name, sessionID.String())

	t.Run("Initialize upload session returns 200 with session ID", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			InitUpload(name).
			Return(&store.UploadSession{Location: "123"}, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.InitUpload(name)

		AssertResponseCode(t, resp, http.StatusAccepted)
		AssertResponseHeader(t, resp, server.HeaderLocation, "123")
	})

	t.Run("Checking active session returns 200", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			StatUpload(name, sessionID.String()).
			Return(&store.BlobInfo{}, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckUpload(location)

		AssertResponseCode(t, resp, http.StatusNoContent)
		AssertResponseHeader(t, resp, server.HeaderLocation, location.Path)
		AssertResponseHeader(t, resp, server.HeaderRange, "0-0")
	})

	t.Run("Checking status of an unknown upload session returns 404 and ErrBlobUploadUnknown", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			StatUpload(name, sessionID.String()).
			Return(nil, repository.ErrBlobUploadUnknown)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckUpload(location)

		AssertResponseCode(t, resp, http.StatusNotFound)
		AssertResponseBodyContainsError(t, resp, repository.ErrBlobUploadUnknown)
	})
}
