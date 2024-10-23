package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/paths"
)

func TestManifests(t *testing.T) {
	store := cascade.NewInMemoryStore()
	service := cascade.NewRegistryService(cascade.NewInMemoryStore())
	server := New(service)

	store.Set(paths.BlobStore.BlobData("sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9"), []byte(`{"mediaType":"something"}`))
	store.Set(paths.MetaStore.ManifestLink("library/fedora", "sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9"), nil)
	store.Set(paths.MetaStore.TagLink("library/fedora", "40"), []byte("sha256:0538c8bd672371fd3bc9eafb2500c046b7334e823b6682a11ea04d843c14cea9"))

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
		assertErrorInResponseBody(t, response.Body, cascade.ErrManifestUnknown)
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
		assertErrorInResponseBody(t, response.Body, cascade.ErrManifestUnknown)
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
		assertErrorInResponseBody(t, response.Body, cascade.ErrManifestUnknown)
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
