package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
)

func TestStatManifests(t *testing.T) {
	t.Run("Stat existing manifest returns 200 with correct size in Content-Length header", func(t *testing.T) {
		name, digest, manifest := randomManifest()
		server := New(&StubRegistryService{statManifest: func(repository, reference string) (*cascade.FileInfo, error) {
			if repository == name && reference == digest.String() {
				return &cascade.FileInfo{Size: int64(len(manifest))}, nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newHeadManifestRequest(name, digest.String())
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertHeader(t, headerContentLength, response.Header(), strconv.Itoa(len(manifest)))
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("Stat non-existent manifest returns 404 and ErrManifestUknown", func(t *testing.T) {
		server := New(&StubRegistryService{statManifest: func(repository, reference string) (*cascade.FileInfo, error) {
			return nil, cascade.ErrManifestUnknown
		}})

		request := newHeadManifestRequest("non/existent", "sha256:c9108d165db831a21e1797902d2fb81d57231978d164752225492585e5ca61b3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, cascade.ErrManifestUnknown)
	})
}

func TestGetManifests(t *testing.T) {
	name, digest, manifest := randomManifest()

	t.Run("Retrieving an existing manifest returns 200", func(t *testing.T) {
		server := New(&StubRegistryService{getManifest: func(repository, reference string) ([]byte, error) {
			if repository == name && reference == digest.String() {
				return manifest, nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newGetManifestRequest(name, digest.String())
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertHeader(t, headerContentType, response.Header(), v1.MediaTypeImageLayer)
		assertResponseBody(t, response.Body.Bytes(), manifest)
	})

	t.Run("Retrieving a manifest by tag returns 200", func(t *testing.T) {
		wantTag := "40"
		server := New(&StubRegistryService{
			getTag: func(repository, tag string) (string, error) {
				if repository == name && tag == wantTag {
					return digest.String(), nil
				}
				panic(errDataNotPassedCorrectly)
			},
			getManifest: func(repository, reference string) ([]byte, error) {
				if repository == name && reference == digest.String() {
					return manifest, nil
				}
				panic(errDataNotPassedCorrectly)
			},
		})

		request := newGetManifestRequest(name, wantTag)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), manifest)
	})

	t.Run("Retrieving a non-existent manifest returns status 404 and ErrManifestUnknown", func(t *testing.T) {
		server := New(&StubRegistryService{getManifest: func(repository, reference string) ([]byte, error) {
			return nil, cascade.ErrManifestUnknown
		}})
		request := newGetManifestRequest("non/existent", "sha256:c9108d165db831a21e1797902d2fb81d57231978d164752225492585e5ca61b3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, cascade.ErrManifestUnknown)
	})
}

func TestPutManifest(t *testing.T) {
	t.Run("Uploading a manifest by digest returns code 201", func(t *testing.T) {
		name, digest, manifest := randomManifest()
		server := New(&StubRegistryService{putManifest: func(repository, reference string, content []byte) error {
			if repository == name && reference == digest.String() && bytes.Equal(content, manifest) {
				return nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newPutManifestRequest(name, digest.String(), manifest)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("Uploading a manifest by tag returns code 201", func(t *testing.T) {
		wantTag := "0.4.2"
		putTagCalled := false
		name, digest, manifest := randomManifest()
		server := New(&StubRegistryService{
			putTag: func(repository, tag, id string) error {
				if repository == name && tag == wantTag && id == digest.String() {
					putTagCalled = true
					return nil
				}
				panic(errDataNotPassedCorrectly)
			},
			putManifest: func(repository, reference string, content []byte) error {
				if repository == name && reference == digest.String() && bytes.Equal(content, manifest) {
					return nil
				}
				panic(errDataNotPassedCorrectly)
			},
		})

		request := newPutManifestRequest(name, wantTag, manifest)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		if !putTagCalled {
			t.Error("service.PutTag was never called")
		}

		assertStatus(t, response.Code, http.StatusCreated)
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("Uploading an invalid manifest returns 400 and ErrManifestInvalid", func(t *testing.T) {
		server := New(&StubRegistryService{putManifest: func(repository, reference string, content []byte) error {
			return cascade.ErrManifestInvalid
		}})

		request := newPutManifestRequest("library/fedora", "123", []byte{})
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
		assertErrorInResponseBody(t, response.Body, cascade.ErrManifestInvalid)
	})
}

func TestDeleteManifest(t *testing.T) {
	t.Run("Deleting a manifest returns code 202", func(t *testing.T) {
		name, digest, _ := randomManifest()
		server := New(&StubRegistryService{deleteManifest: func(repository, reference string) error {
			if repository == name && reference == digest.String() {
				return nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newDeleteManifestRequest(name, digest.String())
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
	})

	t.Run("Deleting an unknown manifest returns 404", func(t *testing.T) {
		server := New(&StubRegistryService{deleteManifest: func(repository, reference string) error {
			return cascade.ErrManifestUnknown
		}})

		request := newDeleteManifestRequest("dont/exist", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, cascade.ErrManifestUnknown)
	})
}

func TestManifestsOthers(t *testing.T) {
	t.Run("Other methods are not allowed", func(t *testing.T) {
		server := New(nil)

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

func randomManifest() (string, digest.Digest, []byte) {
	name := randomName()
	manifest, _ := json.Marshal(v1.Manifest{
		MediaType: v1.MediaTypeImageLayer,
	})
	digest := digest.FromBytes(manifest)

	return name, digest, manifest
}
