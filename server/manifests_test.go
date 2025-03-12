package server

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestStatManifests(t *testing.T) {
	t.Run("Stat existing manifest returns 200 with correct size in Content-Length header", func(t *testing.T) {
		name := RandomName()
		digest, manifest := RandomManifest()
		server := New(&StubRegistryService{statManifest: func(repository, reference string) (*cascade.FileInfo, error) {
			if repository == name && reference == digest.String() {
				return &cascade.FileInfo{Size: int64(len(manifest.Bytes()))}, nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newHeadManifestRequest(name, digest.String())
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusOK)
		AssertResponseHeader(t, response.Result(), headerContentLength, strconv.Itoa(len(manifest.Bytes())))
		AssertResponseBodyEquals(t, response.Result(), nil)
	})

	t.Run("Stat existing manifest by tag returns 200 with correct size in Content-Length header", func(t *testing.T) {
		wantTag := "v0.9.1"
		name := RandomName()
		digest, manifest := RandomManifest()
		server := New(&StubRegistryService{
			getTag: func(repository, tag string) (string, error) {
				if repository == name && tag == wantTag {
					return digest.String(), nil
				}
				panic(errDataNotPassedCorrectly)
			},
			statManifest: func(repository, reference string) (*cascade.FileInfo, error) {
				if repository == name && reference == digest.String() {
					return &cascade.FileInfo{Size: int64(len(manifest.Bytes()))}, nil
				}
				panic(errDataNotPassedCorrectly)
			},
		})

		request := newHeadManifestRequest(name, wantTag)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusOK)
		AssertResponseHeader(t, response.Result(), headerContentLength, strconv.Itoa(len(manifest.Bytes())))
		AssertResponseBodyEquals(t, response.Result(), nil)
	})

	t.Run("Stat non-existent manifest returns 404 and ErrManifestUknown", func(t *testing.T) {
		server := New(&StubRegistryService{statManifest: func(repository, reference string) (*cascade.FileInfo, error) {
			return nil, cascade.ErrManifestUnknown
		}})

		request := newHeadManifestRequest("non/existent", "sha256:c9108d165db831a21e1797902d2fb81d57231978d164752225492585e5ca61b3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusNotFound)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrManifestUnknown)
	})

	t.Run("Stat non-existent manifest by tag returns 404 and ErrManifestUknown", func(t *testing.T) {
		server := New(&StubRegistryService{getTag: func(repository, tag string) (string, error) {
			return "", cascade.ErrManifestUnknown
		}})

		request := newHeadManifestRequest("non/existent", "v1.2.3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusNotFound)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrManifestUnknown)
	})
}

func TestGetManifests(t *testing.T) {
	name := RandomName()
	digest, manifest := RandomManifest()

	t.Run("Retrieving an existing manifest returns 200", func(t *testing.T) {
		server := New(&StubRegistryService{getManifest: func(repository, reference string) (*cascade.Manifest, error) {
			if repository == name && reference == digest.String() {
				return cascade.NewManifest(manifest.Bytes())
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newGetManifestRequest(name, digest.String())
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusOK)
		AssertResponseHeader(t, response.Result(), headerContentType, v1.MediaTypeImageLayer)
		AssertResponseBodyEquals(t, response.Result(), manifest.Bytes())
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
			getManifest: func(repository, reference string) (*cascade.Manifest, error) {
				if repository == name && reference == digest.String() {
					return cascade.NewManifest(manifest.Bytes())
				}
				panic(errDataNotPassedCorrectly)
			},
		})

		request := newGetManifestRequest(name, wantTag)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusOK)
		AssertResponseBodyEquals(t, response.Result(), manifest.Bytes())
	})

	t.Run("Retrieving a non-existent manifest returns status 404 and ErrManifestUnknown", func(t *testing.T) {
		server := New(&StubRegistryService{getManifest: func(repository, reference string) (*cascade.Manifest, error) {
			return nil, cascade.ErrManifestUnknown
		}})
		request := newGetManifestRequest("non/existent", "sha256:c9108d165db831a21e1797902d2fb81d57231978d164752225492585e5ca61b3")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusNotFound)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrManifestUnknown)
	})
}

func TestPutManifest(t *testing.T) {
	t.Run("Uploading a manifest by digest returns code 201", func(t *testing.T) {
		name := RandomName()
		digest, manifest := RandomManifest()
		server := New(&StubRegistryService{putManifest: func(repository, reference string, content []byte) error {
			if repository == name && reference == digest.String() && bytes.Equal(content, manifest.Bytes()) {
				return nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newPutManifestRequest(name, digest.String(), manifest.Bytes())
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusCreated)
		AssertResponseHeaderSet(t, response.Result(), headerLocation)
		AssertResponseBodyEquals(t, response.Result(), nil)
	})

	t.Run("Uploading a manifest by tag returns code 201", func(t *testing.T) {
		wantTag := "0.4.2"
		putTagCalled := false
		name := RandomName()
		digest, manifest := RandomManifest()
		server := New(&StubRegistryService{
			putTag: func(repository, tag, id string) error {
				if repository == name && tag == wantTag && id == digest.String() {
					putTagCalled = true
					return nil
				}
				panic(errDataNotPassedCorrectly)
			},
			putManifest: func(repository, reference string, content []byte) error {
				if repository == name && reference == digest.String() && bytes.Equal(content, manifest.Bytes()) {
					return nil
				}
				panic(errDataNotPassedCorrectly)
			},
		})

		request := newPutManifestRequest(name, wantTag, manifest.Bytes())
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		if !putTagCalled {
			t.Error("service.PutTag was never called")
		}

		AssertResponseCode(t, response.Result(), http.StatusCreated)
		AssertResponseHeaderSet(t, response.Result(), headerLocation)
		AssertResponseBodyEquals(t, response.Result(), nil)
	})

	t.Run("Uploading an invalid manifest returns 400 and ErrManifestInvalid", func(t *testing.T) {
		server := New(&StubRegistryService{putManifest: func(repository, reference string, content []byte) error {
			return cascade.ErrManifestInvalid
		}})

		request := newPutManifestRequest("library/fedora", "123", []byte{})
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusBadRequest)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrManifestInvalid)
	})
}

func TestDeleteManifest(t *testing.T) {
	t.Run("Deleting a manifest returns code 202", func(t *testing.T) {
		name := RandomName()
		digest := RandomDigest()
		server := New(&StubRegistryService{deleteManifest: func(repository, reference string) error {
			if repository == name && reference == digest.String() {
				return nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newDeleteManifestRequest(name, digest.String())
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusAccepted)
	})

	t.Run("Deleting a manifest by tag returns 202", func(t *testing.T) {
		name, wantTag := RandomName(), "v4.2.3"
		deleteTagCalled := false
		server := New(&StubRegistryService{deleteTag: func(repository, tag string) error {
			if repository == name && tag == wantTag {
				deleteTagCalled = true
				return nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request := newDeleteManifestRequest(name, wantTag)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusAccepted)
		if !deleteTagCalled {
			t.Error("DeleteTag was not called")
		}
	})

	t.Run("Deleting an unknown manifest returns 404", func(t *testing.T) {
		server := New(&StubRegistryService{deleteManifest: func(repository, reference string) error {
			return cascade.ErrManifestUnknown
		}})

		request := newDeleteManifestRequest("dont/exist", "sha256:ce5449ab65895b60068d164e81b646753d268583a70895acee51e1d711ddf3a2")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusNotFound)
		AssertResponseBodyContainsError(t, response.Result(), cascade.ErrManifestUnknown)
	})
}

func TestManifestsOthers(t *testing.T) {
	t.Run("Other methods are not allowed", func(t *testing.T) {
		server := New(nil)

		request, _ := http.NewRequest(http.MethodTrace, "/v2/library/fedora/manifests/1.0.0", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusMethodNotAllowed)
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
