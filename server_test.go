package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type StubRegistryStore struct {
	blobStore     map[string]map[string][]byte
	manifestStore map[string]map[string][]byte
}

func (s *StubRegistryStore) StatBlob(name, digest string) bool {
	if _, ok := s.blobStore[name]; ok {
		_, ok := s.blobStore[name][digest]
		return ok
	}
	return false
}

func (s *StubRegistryStore) GetBlob(name, digest string) []byte {
	return s.blobStore[name][digest]
}

func (s *StubRegistryStore) StatManifest(name, reference string) (bool, int) {
	if _, ok := s.manifestStore[name]; ok {
		val, ok := s.manifestStore[name][reference]
		return ok, len(val)
	}
	return false, 0
}

func (s *StubRegistryStore) GetManifest(name, reference string) []byte {
	return s.manifestStore[name][reference]
}

func TestManifests(t *testing.T) {
	store := newStubRegistryStore()
	server := NewRegistryServer(store)

	t.Run("Test HEAD /manifests", func(t *testing.T) {
		request := newHeadManifestRequest("library/fedora", "1.0.0")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertHeader(t, "Content-Length", response.Header(), "25")
		assertResponseBody(t, response.Body.Bytes(), nil)

		request = newHeadManifestRequest("non/existent", "1.0.0")
		response = httptest.NewRecorder()
		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertResponseBody(t, response.Body.Bytes(), nil)
	},
	)

	t.Run("Test GET /manifests", func(t *testing.T) {
		request := newGetManifestRequest("library/fedora", "1.0.0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		var got v1.Manifest

		err := json.NewDecoder(response.Body).Decode(&got)

		if err != nil {
			t.Fatalf("Unable to parse response %q from server into %T", response.Body, got)
		}

		assertStatus(t, response.Code, http.StatusOK)
		assertHeader(t, "Content-Type", response.Header(), "something")
	})

	t.Run("put manifest returns 201", func(t *testing.T) {
		request := newPutManifestRequest("library/fedora", "1.0.0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
	})

	t.Run("delete manifest returns 202", func(t *testing.T) {
		request := newDeleteManifestRequest("library/fedora", "1.0.0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
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

func newPutManifestRequest(name, reference string) *http.Request {
	req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("/v2/%s/manifests/%s", name, reference), nil)
	return req
}

func newDeleteManifestRequest(name, reference string) *http.Request {
	req, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("/v2/%s/manifests/%s", name, reference), nil)
	return req
}

func TestGetBlob(t *testing.T) {
	store := newStubRegistryStore()
	server := NewRegistryServer(store)
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

	t.Run("check if blob exists", func(t *testing.T) {
		request := newCheckBlobRequest("library/fedora", "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("returns 404 on missing blob", func(t *testing.T) {
		request := newGetBlobRequest("library/fedora", "sha256:sha256:ee0235dbf464273241b2bb74b883b4f1a6bf6d8c324b7e51d1eb0a2fb6539fdc")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
	})
}

func newStubRegistryStore() *StubRegistryStore {
	return &StubRegistryStore{
		manifestStore: map[string]map[string][]byte{
			"library/fedora": {
				"1.0.0": []byte(`{"mediaType":"something"}`),
			},
		},
		blobStore: map[string]map[string][]byte{
			"library/fedora": {
				"sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b": []byte("my blob content"),
				"sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9": []byte("my other blob content"),
			},
			"containers/skopeo": {
				"sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64": []byte("skopeo container content"),
			},
		},
	}
}

func newGetBlobRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
	return req
}

func newCheckBlobRequest(name, digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("/v2/%s/blobs/%s", name, digest), nil)
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

func assertResponseBody(t *testing.T, got, want []byte) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
