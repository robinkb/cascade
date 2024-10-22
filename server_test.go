package main

import (
	"bytes"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry/paths"
)

func TestRoot(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	t.Run("GET /v2/ should return 200", func(t *testing.T) {
		request, _ := http.NewRequest(http.MethodGet, "/v2/", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
	})
}

func TestManifests(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	service.store.Set(paths.BlobStore.BlobData("sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9"), []byte(`{"mediaType":"something"}`))
	service.store.Set(paths.MetaStore.ManifestLink("library/fedora", "sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9"), nil)
	service.store.Set(paths.MetaStore.TagLink("library/fedora", "40"), []byte("sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9"))

	t.Run("Stat existing manifest", func(t *testing.T) {
		request := newHeadManifestRequest("library/fedora", "sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertHeader(t, headerContentLength, response.Header(), "25")
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("Stat non-existent manifest", func(t *testing.T) {
		request := newHeadManifestRequest("non/existent", "sha256:c9108d165db831a21e1797902d2fb81d57231978d164752225492585e5ca61b3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrManifestUnknown)
	})

	t.Run("Fetch an existing manifest", func(t *testing.T) {
		request := newGetManifestRequest("library/fedora", "sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		// TODO: This should be fixed :)
		assertHeader(t, headerContentType, response.Header(), "something")

		var got v1.Manifest
		err := json.NewDecoder(response.Body).Decode(&got)
		if err != nil {
			t.Fatalf("Unable to parse response %q from server into %T", response.Body, got)
		}
	})

	t.Run("Fetch a non-existent manifest", func(t *testing.T) {
		request := newGetManifestRequest("non/existent", "sha256:c9108d165db831a21e1797902d2fb81d57231978d164752225492585e5ca61b3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrManifestUnknown)
	})

	t.Run("Upload a manifest", func(t *testing.T) {
		manifest := []byte(`{"mediaType":"something"}`)
		digest := "sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9"
		request := newPutManifestRequest("library/fedora", digest, manifest)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)

		request = newGetManifestRequest("library/fedora", digest)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)
		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), manifest)
	})

	t.Run("Upload a manifest with invalid content", func(t *testing.T) {
		// TODO: Fix
		t.SkipNow()

		manifest := []byte(`blabla`)
		digest := "sha256:ccadd99b16cd3d200c22d6db45d8b6630ef3d936767127347ec8a76ab992c2ea"
		request := newPutManifestRequest("library/fedora", digest, manifest)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Delete manifest and make sure it's not retrievable", func(t *testing.T) {
		repository := "g/h"
		content := randomContents(32)
		digest := fmt.Sprintf("sha256:%x", sha256.Sum256(content))

		service.PutManifest(repository, digest, content)

		request := newHeadManifestRequest(repository, digest)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)

		request = newDeleteManifestRequest(repository, digest)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)

		request = newHeadManifestRequest(repository, digest)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
	})

	t.Run("Deleting an unknown manifest returns an error", func(t *testing.T) {
		request := newDeleteManifestRequest("dont/exist", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrManifestUnknown)
	})

	t.Run("Get a manifest by tag", func(t *testing.T) {
		request := newGetManifestRequest("library/fedora", "40")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte(`{"mediaType":"something"}`))
	})

	t.Run("Upload a manifest by tag", func(t *testing.T) {
		content := []byte(`{"mediaType":"something.else"}`)
		request := newPutManifestRequest("library/fedora", "41", content)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
	})

	t.Run("Other methods are not allowed", func(t *testing.T) {
		request, _ := http.NewRequest(http.MethodTrace, "/v2/library/fedora/manifests/1.0.0", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusMethodNotAllowed)
	})

}

func newHeadManifestRequest(name, reference string) *http.Request {
	req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("/v2/%s/manifests/%s", name, reference), nil)
	return req
}

func newGetManifestRequest(name, reference string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/manifests/%s", name, reference), nil)
	return req
}

func newPutManifestRequest(name, reference string, body []byte) *http.Request {
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("/v2/%s/manifests/%s", name, reference), bytes.NewBuffer(body))
	return req
}

func newDeleteManifestRequest(name, reference string) *http.Request {
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("/v2/%s/manifests/%s", name, reference), nil)
	return req
}

func TestStatBlob(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	service.store.Set(paths.MetaStore.LayerLink("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), []byte("my layer content"))
	service.store.Set(paths.BlobStore.BlobData("sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9/data"), []byte("my other layer content"))

	t.Run("check if layer exists", func(t *testing.T) {
		request := newCheckLayerRequest("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("known layer in unknown repository returns 404", func(t *testing.T) {
		request := newCheckLayerRequest("not/known", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUnknown)
	})

	t.Run("unknown layer returns 404", func(t *testing.T) {
		request := newCheckLayerRequest("library/fedora", "sha256:8029119ed9bf9b748a2233d78e7e124b5c923e1c20a4ec4ea2176b303d2121fa")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUnknown)
	})
}

func assertErrorInResponseBody(t *testing.T, body *bytes.Buffer, want Error) {
	t.Helper()

	var errs ErrorResponse
	err := json.NewDecoder(body).Decode(&errs)
	assertNoError(t, err)

	for _, err := range errs.Errors {
		if errors.Is(err, want) {
			return
		}
	}

	t.Errorf("could not find error in response; got %v, want %v", body, want)
}

func TestGetLayer(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	service.store.Set(paths.MetaStore.LayerLink("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), []byte("my layer content"))
	service.store.Set(paths.MetaStore.LayerLink("library/fedora", "sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9"), []byte("my other layer content"))
	service.store.Set(paths.MetaStore.LayerLink("containers/skopeo", "sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64"), []byte("skopeo container content"))

	t.Run("get layer for library/fedora", func(t *testing.T) {
		request := newGetLayerRequest("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte("my layer content"))
	})

	t.Run("get other layer for library/fedora", func(t *testing.T) {
		request := newGetLayerRequest("library/fedora", "sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte("my other layer content"))
	})

	t.Run("get layer for containers/skopeo", func(t *testing.T) {
		request := newGetLayerRequest("containers/skopeo", "sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte("skopeo container content"))
	})

	t.Run("returns 404 on unknown layer", func(t *testing.T) {
		request := newGetLayerRequest("library/fedora", "sha256:sha256:ee0235dbf464273241b2bb74b883b4f1a6bf6d8c324b7e51d1eb0a2fb6539fdc")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUnknown)
	})
}

func TestLayerUploadSession(t *testing.T) {
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

func TestLayerUploadsMonolithic(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	t.Run("Monolithic layer upload - happy path", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)

		request := newLayerUploadRequest(session.Location, content)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)
		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})

	t.Run("Uploading without session returns 404", func(t *testing.T) {
		request := newLayerUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUploadUnknown)
	})

	t.Run("Uploading without required headers returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newLayerUploadRequest(session.Location, content)
		request.Header.Del(headerContentType)
		request.Header.Del(headerContentLength)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Uploading without digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newLayerUploadRequest(session.Location, content)
		request.URL.RawQuery = ""
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Uploading with invalid digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newLayerUploadRequest(session.Location, content)
		request.URL.RawQuery = "digest=blablabla"
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Uploading with wrong digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newLayerUploadRequest(session.Location, content)
		response := httptest.NewRecorder()

		otherContent := randomContents(64)
		id := digest.FromBytes(otherContent)
		query := request.URL.Query()
		query.Set("digest", id.String())
		request.URL.RawQuery = query.Encode()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
		assertErrorInResponseBody(t, response.Body, ErrBlobUploadInvalid)
	})
}

func TestBlobUploadsChunked(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	t.Run("Chunked upload happy path", func(t *testing.T) {
		// Initialize the upload session by obtaining an ID.
		// For chunked uploads, header Content-Length: 0 must be set.
		request := newInitUploadRequest("library/fedora")
		request.Header.Add(headerContentLength, "0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)

		content := randomContents(16 * 1024)
		digest := digest.FromBytes(content)
		r := bytes.NewReader(content)
		buffer := make([]byte, 1*1024)
		written := 0

		for {
			n, err := io.ReadFull(r, buffer)
			assertNoError(t, err)

			request = newUploadChunkRequest(location, buffer, written)
			response = httptest.NewRecorder()

			server.ServeHTTP(response, request)

			written += n

			assertStatus(t, response.Code, http.StatusAccepted)
			assertHeaderSet(t, headerLocation, response.Header())
			assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))

			if r.Len() == 0 {
				break
			}

			location = response.Header().Get(headerLocation)
		}

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

	t.Run("Chunked upload with dyscalculic client (gets the ranges wrong)", func(t *testing.T) {
		request := newInitUploadRequest("library/fedora")
		request.Header.Add(headerContentLength, "0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)

		location := response.Header().Get(headerLocation)

		// Prepare content to upload
		content := randomContents(2 * 1024)
		digest := digest.FromBytes(content)
		r := bytes.NewReader(content)
		buffer := make([]byte, 1*1024)
		written := 0

		n, err := io.ReadFull(r, buffer)
		assertNoError(t, err)

		// Do a proper upload
		request = newUploadChunkRequest(location, buffer, written)
		response = httptest.NewRecorder()
		server.ServeHTTP(response, request)
		written += n

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))

		_, err = io.ReadFull(r, buffer)
		assertNoError(t, err)

		request = newUploadChunkRequest(location, buffer, written)
		// Mess up the start of the content range
		request.Header.Set(headerContentRange, fmt.Sprintf("1-%d", written))
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusRequestedRangeNotSatisfiable)

		// Check our upload status, confirm nothing is written.
		request = newCheckUploadRequest(location)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))
		r.Seek(int64(written), 0)

		// Try uploading the chunk again, this time missing up
		// the end of the content range.
		_, err = io.ReadFull(r, buffer)
		assertNoError(t, err)

		request = newUploadChunkRequest(location, buffer, written)
		request.Header.Set(headerContentRange, fmt.Sprintf("%d-1", written))
		response = httptest.NewRecorder()
		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusRequestedRangeNotSatisfiable)

		// Check our upload status, confirm nothing is written.
		request = newCheckUploadRequest(location)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))
		r.Seek(int64(written), 0)

		// Do it properly this time.
		n, err = io.ReadFull(r, buffer)
		assertNoError(t, err)

		request = newUploadChunkRequest(location, buffer, written)
		response = httptest.NewRecorder()
		server.ServeHTTP(response, request)
		written += n

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))

		// And close the upload.
		request = newCloseUploadRequest(location, digest.String(), nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location = response.Header().Get(headerLocation)

		// Verify that the content was uploaded successfully.
		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})
}

func TestBlobUploadsStreamed(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	t.Run("Streamed upload happy path", func(t *testing.T) {
		// Initialize the upload session by obtaining an ID.
		request := newInitUploadRequest("library/fedora")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)

		content := randomContents(32 * 1024)
		digest := digest.FromBytes(content)
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

func newInitUploadRequest(name string) *http.Request {
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("/v2/%s/blobs/uploads/", name), nil)
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
	req.Header.Set(headerContentRange, fmt.Sprintf("%d-%d", written, written+size))
	req.Header.Set(headerContentLength, strconv.Itoa(size))
	return req
}

func newCloseUploadRequest(location, digest string, content []byte) *http.Request {
	var body io.Reader
	if len(content) > 0 {
		body = bytes.NewBuffer(content)
	}

	req, _ := http.NewRequest(http.MethodPut, location, body)
	query := req.URL.Query()
	query.Set("digest", digest)
	req.URL.RawQuery = query.Encode()
	return req
}

func newGetLayerRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
	return req
}

func newCheckLayerRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
	return req
}

func newLayerUploadRequest(location string, content []byte) *http.Request {
	id := digest.FromBytes(content)

	req, _ := http.NewRequest(http.MethodPut, location, bytes.NewBuffer(content))
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerContentLength, fmt.Sprint(len(content)))

	query := req.URL.Query()
	query.Set("digest", id.String())
	req.URL.RawQuery = query.Encode()
	return req
}

func assertStatus(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("got status %d, want %d", got, want)
	}
}

func assertHeader(t *testing.T, header string, got http.Header, want string) {
	t.Helper()
	val := got.Get(header)
	if val == "" {
		t.Errorf("Header '%s' not set", header)
		return
	}

	if val != want {
		t.Errorf("Header '%s' set to %q, want %q", header, val, want)
	}
}

func assertHeaderSet(t *testing.T, header string, got http.Header) {
	t.Helper()
	if got.Get(header) == "" {
		t.Errorf("Header '%s' not set", header)
	}
}

func assertResponseBody(t *testing.T, got, want []byte) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("response body did not match the expected content")
	}
}

func randomContents(length int64) []byte {
	data := make([]byte, length)
	cryptorand.Read(data)
	return data
}
