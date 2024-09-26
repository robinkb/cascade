package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type StubRegistryStore struct {
	blobs map[string][]byte
}

func (s *StubRegistryStore) GetBlob(digest string) []byte {
	return s.blobs[digest]
}

func newCheckBlobRequest(digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("/v2/library/fedora/blobs/%s", digest), nil)
	return req
}

func TestGetBlob(t *testing.T) {
	store := &StubRegistryStore{
		map[string][]byte{
			"sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b": []byte("my blob content"),
			"sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9": []byte("my other blob content"),
		},
	}
	server := &RegistryServer{store}
	t.Run("get blob", func(t *testing.T) {
		request := newGetBlobRequest("sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte("my blob content"))
	})

	t.Run("get other blob", func(t *testing.T) {
		request := newGetBlobRequest("sha256:d0dc9f3a77cfc4c7d8408016c721d12559fcc40a07aca3826622f68fe6215aa9")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), []byte("my other blob content"))
	})

	t.Run("check if blob exists", func(t *testing.T) {
		request := newCheckBlobRequest("sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), nil)
	})

	t.Run("returns 404 on missing blob", func(t *testing.T) {
		request := newGetBlobRequest("sha256:sha256:ee0235dbf464273241b2bb74b883b4f1a6bf6d8c324b7e51d1eb0a2fb6539fdc")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
	})
}

func newGetBlobRequest(digest string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/library/fedora/blobs/%s", digest), nil)
	return req
}

func assertStatus(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("got status %d, want %d", got, want)
	}
}

func assertResponseBody(t *testing.T, got, want []byte) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
