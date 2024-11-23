package server

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
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

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, cascade.ErrBlobUploadUnknown)
	})

	t.Run("Uploading without required headers returns 400", func(t *testing.T) {
		server := New(&StubRegistryService{})
		content := randomContents(32)

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", content)
		request.Header.Del(headerContentType)
		request.Header.Del(headerContentLength)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Closing upload without digest returns 400", func(t *testing.T) {
		server := New(&StubRegistryService{})

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		request.URL.RawQuery = ""
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Closing upload with invalid digest returns 400", func(t *testing.T) {
		server := New(&StubRegistryService{closeUpload: func(repository, id, digest string) error {
			return cascade.ErrDigestInvalid
		}})

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		request.URL.RawQuery = "digest=blablabla"
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
		assertErrorInResponseBody(t, response.Body, cascade.ErrDigestInvalid)
	})

	t.Run("Uploading with wrong digest returns 400", func(t *testing.T) {
		server := New(&StubRegistryService{closeUpload: func(repository, id, digest string) error {
			return cascade.ErrBlobUploadInvalid
		}})

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
		assertErrorInResponseBody(t, response.Body, cascade.ErrBlobUploadInvalid)
	})
}

func TestBlobUploadsChunked(t *testing.T) {
	// TODO: This used to have integration-style tests that have been moved
	// to the conformance test. There should be more basic handler unit tests here.
}

func TestBlobUploadsStreamed(t *testing.T) {
	server := newTestServer()

	t.Run("Streamed upload happy path", func(t *testing.T) {
		name, digest, content := randomBlob(32 * 1024)

		// Initialize the upload session by obtaining an ID.
		request := newInitUploadRequest(name)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)

		r := bytes.NewReader(content)

		request, _ = http.NewRequest(http.MethodPatch, location, r)
		request.Header.Set(headerContentType, contentTypeOctetStream)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)

		request = newCloseUploadRequest(location, digest.String(), nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location = response.Header().Get(headerLocation)

		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})
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

func newUploadChunkRequest(location string, content []byte, written int) *http.Request {
	size := len(content)
	buf := bytes.NewBuffer(content)
	req, _ := http.NewRequest(http.MethodPatch, location, buf)
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerContentRange, fmt.Sprintf("%d-%d", written, written+size-1))
	req.Header.Set(headerContentLength, strconv.Itoa(size))
	return req
}

func newCloseUploadRequest(location, digest string, content []byte) *http.Request {
	body := bytes.NewBuffer(content)
	req, _ := http.NewRequest(http.MethodPut, location, body)
	if len(content) > 0 {
		req.Header.Set(headerContentType, contentTypeOctetStream)
		req.Header.Set(headerContentLength, strconv.Itoa(len(content)))
	}
	query := req.URL.Query()
	query.Set("digest", digest)
	req.URL.RawQuery = query.Encode()
	return req
}
