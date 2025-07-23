package server

import (
	"net/http"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/robinkb/cascade-registry/repository"
	"github.com/robinkb/cascade-registry/store"
	. "github.com/robinkb/cascade-registry/testing"
	testclient "github.com/robinkb/cascade-registry/testing/client"
	"github.com/robinkb/cascade-registry/testing/mock"
)

func TestBlobUploadSession(t *testing.T) {
	name := RandomName()
	sessionID, _ := uuid.NewV7()
	location := newLocation(name, sessionID.String())

	t.Run("Initialize upload session returns 200 with session ID", func(t *testing.T) {
		uuid := RandomUUID()
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			InitUpload(name).
			Return(&store.UploadSession{ID: uuid}, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.InitUpload(name)

		AssertResponseCode(t, resp, http.StatusAccepted)
		AssertResponseHeader(t, resp, HeaderLocation, Location(name, uuid.String()))
	})

	t.Run("Checking active session returns 200", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			StatUpload(name, sessionID.String()).
			Return(&store.BlobInfo{}, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.CheckUpload(location)

		AssertResponseCode(t, resp, http.StatusNoContent)
		AssertResponseHeader(t, resp, HeaderLocation, location.Path)
		AssertResponseHeader(t, resp, HeaderRange, "0-0")
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

	t.Run("Other methods are not allowed", func(t *testing.T) {
		client := testclient.NewTestClientForHandler(t, New(nil))
		resp := client.Do(http.MethodConnect, "/v2/my/repo/blobs/uploads/", nil, nil)
		AssertResponseCode(t, resp, http.StatusMethodNotAllowed)
	})
}
