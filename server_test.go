package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
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

	t.Run("Test HEAD /manifests", func(t *testing.T) {
		request := newHeadManifestRequest("library/fedora", "sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertHeader(t, headerContentLength, response.Header(), "25")
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("Test HEAD /manifests on non-existent manifest", func(t *testing.T) {
		request := newHeadManifestRequest("non/existent", "sha256:c9108d165db831a21e1797902d2fb81d57231978d164752225492585e5ca61b3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrManifestUnknown)
	})

	t.Run("Test GET /manifests", func(t *testing.T) {
		request := newGetManifestRequest("library/fedora", "sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertHeader(t, headerContentType, response.Header(), "something")

		var got v1.Manifest
		err := json.NewDecoder(response.Body).Decode(&got)
		if err != nil {
			t.Fatalf("Unable to parse response %q from server into %T", response.Body, got)
		}
	})

	t.Run("Test GET /manifests on non-existent manifest", func(t *testing.T) {
		request := newGetManifestRequest("non/existent", "sha256:c9108d165db831a21e1797902d2fb81d57231978d164752225492585e5ca61b3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrManifestUnknown)
	})

	t.Run("Test PUT /manifests", func(t *testing.T) {
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

	t.Run("Test PUT /manifests with invalid content", func(t *testing.T) {
		// TODO: Fix
		t.SkipNow()

		manifest := []byte(`blabla`)
		digest := "sha256:ccadd99b16cd3d200c22d6db45d8b6630ef3d936767127347ec8a76ab992c2ea"
		request := newPutManifestRequest("library/fedora", digest, manifest)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("delete manifest returns 202 and is not retrievable", func(t *testing.T) {
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

	t.Run("delete non-existent manifest returns 404", func(t *testing.T) {
		request := newDeleteManifestRequest("dont/exist", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrManifestUnknown)
	})

	t.Run("other methods return 405", func(t *testing.T) {
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

func TestGetBlob(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	service.store.Set(paths.MetaStore.LayerLink("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"), []byte("my blob content"))
	service.store.Set(paths.MetaStore.LayerLink("library/fedora", "sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9"), nil)
	service.store.Set(paths.BlobStore.BlobData("sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9"), []byte("my other blob content"))
	service.store.Set(paths.MetaStore.LayerLink("containers/skopeo", "sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64"), nil)
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

	t.Run("returns 404 on missing blob", func(t *testing.T) {
		request := newGetBlobRequest("library/fedora", "sha256:sha256:ee0235dbf464273241b2bb74b883b4f1a6bf6d8c324b7e51d1eb0a2fb6539fdc")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUnknown)
	})
}

func TestBlobUploadsMonolithic(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	t.Run("POST /blobs/uploads/", func(t *testing.T) {
		// Initialize the upload session by obtaining an ID.
		repository := "library/fedora"
		request, _ := http.NewRequest(http.MethodPost, "/v2/library/fedora/blobs/uploads/", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeaderSet(t, headerLocation, response.Header())
		location := response.Header().Get(headerLocation)
		u, err := url.Parse(location)
		if err != nil {
			t.Errorf("failed to parse Location header %q: %v", location, err)
		}

		segments := strings.Split(u.Path, "/")
		sessionId := segments[len(segments)-1]

		_, err = server.service.StatUpload(repository, sessionId)
		assertNoError(t, err)
	})

	t.Run("PUT /blobs/uploads/{reference} happy path", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)

		request := newBlobUploadRequest(session.Location, content)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)

		request, _ = http.NewRequest(http.MethodHead, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)

		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})

	t.Run("PUT /blobs/uploads/{reference} without session returns 404", func(t *testing.T) {
		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, ErrBlobUploadUnknown)
	})

	t.Run("PUT /blobs/uploads/{reference} without required headers returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newBlobUploadRequest(session.Location, content)
		request.Header.Del(headerContentType)
		request.Header.Del(headerContentLength)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("PUT /blobs/uploads/{reference} without digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newBlobUploadRequest(session.Location, content)
		request.URL.RawQuery = ""
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("PUT /blobs/uploads/{reference} with invalid digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newBlobUploadRequest(session.Location, content)
		request.URL.RawQuery = "digest=blablabla"
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("PUT /blobs/uploads/{reference} with wrong digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newBlobUploadRequest(session.Location, content)
		response := httptest.NewRecorder()

		otherContent := randomContents(64)
		id := digest.FromBytes(otherContent)
		query := request.URL.Query()
		query.Set("digest", id.String())
		request.URL.RawQuery = query.Encode()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})
}

func TestBlobUploadsChunked(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())
	server := NewRegistryServer(service)

	t.Run("PATCH /blobs/uploads/{reference} happy path", func(t *testing.T) {
		// Initialize the upload session by obtaining an ID.
		// For chunked uploads, header Content-Length: 0 must be set.
		request, _ := http.NewRequest(http.MethodPost, "/v2/library/fedora/blobs/uploads/", nil)
		request.Header.Add(headerContentLength, "0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)

		content := randomContents(32 * 1024 * 1024)
		digest := digest.FromBytes(content)
		buffer := bytes.NewBuffer(content)
		patchSize := 1 << 20
		written := 0

		for {
			patchBuffer := buffer.Next(patchSize)
			request, _ = http.NewRequest(http.MethodPatch, location, bytes.NewBuffer(patchBuffer))
			request.Header.Set(headerContentType, contentTypeOctetStream)
			request.Header.Set("Content-Range", fmt.Sprintf("%d-%d", written, written+patchSize))
			request.Header.Set("Content-Length", strconv.Itoa(patchSize))

			response = httptest.NewRecorder()

			server.ServeHTTP(response, request)

			assertStatus(t, response.Code, http.StatusAccepted)
			assertHeaderSet(t, headerLocation, response.Header())

			written += patchSize
			if buffer.Len() == 0 {
				break
			}

			location = response.Header().Get(headerLocation)
		}

		request, _ = http.NewRequest(http.MethodPut, location, nil)
		query := request.URL.Query()
		query.Add("digest", digest.String())
		request.URL.RawQuery = query.Encode()

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
	rand.Read(data)
	return data
}
