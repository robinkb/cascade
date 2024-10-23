package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBlobUploadSession(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	t.Run("Initialize upload session and check its status", func(t *testing.T) {
		// Initialize the upload session by obtaining an ID.
		repository := "library/fedora"
		request := newInitUploadRequest(repository)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)

		request = newCheckUploadRequest(location)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNoContent)
		assertHeader(t, headerLocation, response.Header(), location)
		assertHeader(t, headerRange, response.Header(), "0-0")
	})

	t.Run("Checking status of an unknown upload session returns 404 and ErrBlobUploadUnknown", func(t *testing.T) {
		request := newCheckUploadRequest("/v2/library/fedora/blobs/uploads/123")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUploadUnknown)
	})

	t.Run("Upload session status with some content written returns correct Range header", func(t *testing.T) {
		request := newInitUploadRequest("library/fedora")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)

	})
}

func newInitUploadRequest(name string) *http.Request {
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/v2/%s/blobs/uploads/", name), nil)
	return req
}
