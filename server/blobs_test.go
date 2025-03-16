package server_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestStatBlob(t *testing.T) {
	t.Run("known blob returns 200", func(t *testing.T) {
		size := 42
		srv := server.New(&StubRegistryService{
			statBlob: func(repository, digest string) (*cascade.FileInfo, error) {
				return &cascade.FileInfo{
					Size: 42,
				}, nil
			},
		})

		request := newCheckBlobRequest("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		srv.ServeHTTP(response, request)
		AssertResponseCode(t, response.Result(), http.StatusOK)
		AssertResponseHeader(t, response.Result(), server.HeaderContentLength, strconv.Itoa(size))
		AssertResponseBodyEquals(t, response.Result(), nil)
	})

	t.Run("unknown blob returns 404", func(t *testing.T) {
		srv := server.New(&StubRegistryService{
			statBlob: func(repository, digest string) (*cascade.FileInfo, error) {
				return nil, cascade.ErrBlobUnknown
			},
		})

		request := newCheckBlobRequest("library/fedora", "sha256:8029119ed9bf9b748a2233d78e7e124b5c923e1c20a4ec4ea2176b303d2121fa")
		response := httptest.NewRecorder()

		srv.ServeHTTP(response, request)
		AssertResponseCode(t, response.Result(), http.StatusNotFound)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrBlobUnknown)
	})
}

func TestGetBlob(t *testing.T) {
	t.Run("Get blob returns 200", func(t *testing.T) {
		content := RandomContents(32)
		srv := server.New(&StubRegistryService{
			getBlob: func(repository, digest string) (io.Reader, error) {
				return bytes.NewBuffer(content), nil
			},
		})

		request := newGetBlobRequest("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		srv.ServeHTTP(response, request)
		AssertResponseCode(t, response.Result(), http.StatusOK)
		AssertResponseBodyEquals(t, response.Result(), content)
	})

	t.Run("returns 404 on unknown blob", func(t *testing.T) {
		srv := server.New(&StubRegistryService{
			getBlob: func(repository, digest string) (io.Reader, error) {
				return nil, cascade.ErrBlobUnknown
			},
		})

		request := newGetBlobRequest("library/fedora", "sha256:sha256:ee0235dbf464273241b2bb74b883b4f1a6bf6d8c324b7e51d1eb0a2fb6539fdc")
		response := httptest.NewRecorder()

		srv.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusNotFound)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrBlobUnknown)
	})

}

func TestDeleteBlob(t *testing.T) {
	t.Run("Delete blob returns 202", func(t *testing.T) {
		name := RandomName()
		id := RandomDigest()

		srv := server.New(&StubRegistryService{
			deleteBlob: func(repository, digest string) error {
				if repository != name || digest != id.String() {
					t.Error("parameters not passed correctly")
				}
				return nil
			},
		})

		request := newDeleteBlobRequest(name, id.String())
		response := httptest.NewRecorder()

		srv.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusAccepted)
	})
}

func TestBlobsOthers(t *testing.T) {
	t.Run("other methods return 405", func(t *testing.T) {
		srv := server.New(nil)

		request := newGetBlobRequest("a", "b")
		request.Method = http.MethodConnect
		response := httptest.NewRecorder()

		srv.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusMethodNotAllowed)
	})
}

func newCheckBlobRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
	return req
}

func newGetBlobRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
	return req
}

func newDeleteBlobRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
	return req
}
