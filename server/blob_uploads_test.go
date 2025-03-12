package server

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestBlobUploadsMonolithic(t *testing.T) {
	// TODO: This used to have integration-style tests that have been moved
	// to the conformance test. There should be more basic handler unit tests here
	// for the happy scenarios.

	t.Run("Uploading without session returns 404", func(t *testing.T) {
		server := New(&StubRegistryService{closeUpload: func(repository, id, digest string) error {
			return cascade.ErrBlobUploadUnknown
		}})

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusNotFound)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrBlobUploadUnknown)
	})

	t.Run("Uploading without required headers returns 400", func(t *testing.T) {
		server := New(&StubRegistryService{})
		content := RandomContents(32)

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", content)
		request.Header.Del(headerContentType)
		request.Header.Del(headerContentLength)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusBadRequest)
	})

	t.Run("Closing upload without digest returns 400", func(t *testing.T) {
		server := New(&StubRegistryService{})

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		request.URL.RawQuery = ""
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusBadRequest)
	})

	t.Run("Closing upload with invalid digest returns 400", func(t *testing.T) {
		server := New(&StubRegistryService{closeUpload: func(repository, id, digest string) error {
			return cascade.ErrDigestInvalid
		}})

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		request.URL.RawQuery = "digest=blablabla"
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusBadRequest)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrDigestInvalid)
	})

	t.Run("Uploading with wrong digest returns 400", func(t *testing.T) {
		server := New(&StubRegistryService{closeUpload: func(repository, id, digest string) error {
			return cascade.ErrBlobUploadInvalid
		}})

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusBadRequest)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrBlobUploadInvalid)
	})
}

func TestBlobUploadsChunked(t *testing.T) {
	// TODO: This used to have integration-style tests that have been moved
	// to the conformance test. There should be more basic handler unit tests here.
}

func TestBlobUploadsStreamed(t *testing.T) {
	// TODO: This used to have integration-style tests that have been moved
	// to the conformance test. There should be more basic handler unit tests here.
}

func newBlobUploadRequest(location string, content []byte) *http.Request {
	id := digest.FromBytes(content)

	req, _ := http.NewRequest(http.MethodPut, location, bytes.NewBuffer(content))
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerContentLength, fmt.Sprint(len(content)))

	query := req.URL.Query()
	query.Set("digest", id.String())
	req.URL.RawQuery = query.Encode()
	return req
}

func newCheckUploadRequest(location string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, location, nil)
	return req
}
