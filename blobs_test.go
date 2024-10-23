package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/paths"
)

func TestStatBlob(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	service.store.Set(paths.MetaStore.BlobLink("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), []byte("my blob content"))
	service.store.Set(paths.BlobStore.BlobData("sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9/data"), []byte("my other blob content"))

	t.Run("check if blob exists", func(t *testing.T) {
		request := newCheckBlobRequest("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("known blob in unknown repository returns 404", func(t *testing.T) {
		request := newCheckBlobRequest("not/known", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUnknown)
	})

	t.Run("unknown blob returns 404", func(t *testing.T) {
		request := newCheckBlobRequest("library/fedora", "sha256:8029119ed9bf9b748a2233d78e7e124b5c923e1c20a4ec4ea2176b303d2121fa")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUnknown)
	})
}

func TestGetBlob(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	service.store.Set(paths.MetaStore.BlobLink("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), []byte("my blob content"))
	service.store.Set(paths.MetaStore.BlobLink("library/fedora", "sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9"), []byte("my other blob content"))
	service.store.Set(paths.MetaStore.BlobLink("containers/skopeo", "sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64"), []byte("skopeo container content"))

	t.Run("get blob for library/fedora", func(t *testing.T) {
		request := newGetBlobRequest("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte("my blob content"))
	})

	t.Run("get other blob for library/fedora", func(t *testing.T) {
		request := newGetBlobRequest("library/fedora", "sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte("my other blob content"))
	})

	t.Run("get blob for containers/skopeo", func(t *testing.T) {
		request := newGetBlobRequest("containers/skopeo", "sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte("skopeo container content"))
	})

	t.Run("returns 404 on unknown blob", func(t *testing.T) {
		request := newGetBlobRequest("library/fedora", "sha256:sha256:ee0235dbf464273241b2bb74b883b4f1a6bf6d8c324b7e51d1eb0a2fb6539fdc")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUnknown)
	})
}

func newGetBlobRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
	return req
}

func newCheckBlobRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
	return req
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
