package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestBlobUploadSession(t *testing.T) {
	server := newTestServer()

	t.Run("Initialize upload session and check its status", func(t *testing.T) {
		// Initialize the upload session by obtaining an ID.
		repository := "library/fedora"
		request := newInitUploadRequest(repository)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusAccepted)
		AssertResponseHeaderSet(t, response.Result(), headerLocation)

		location := response.Header().Get(headerLocation)

		request = newCheckUploadRequest(location)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusNoContent)
		AssertResponseHeader(t, response.Result(), headerLocation, location)
		AssertResponseHeader(t, response.Result(), headerRange, "0-0")
	})

	t.Run("Checking status of an unknown upload session returns 404 and ErrBlobUploadUnknown", func(t *testing.T) {
		request := newCheckUploadRequest("/v2/library/fedora/blobs/uploads/123")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusNotFound)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrBlobUploadUnknown)
	})

	t.Run("Upload session status with some content written returns correct Range header", func(t *testing.T) {
		request := newInitUploadRequest("library/fedora")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusAccepted)
	})
}

func newInitUploadRequest(name string) *http.Request {
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/v2/%s/blobs/uploads/", name), nil)
	return req
}
