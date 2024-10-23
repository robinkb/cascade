package server

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"

	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/robinkb/cascade-registry"
)

func TestRoot(t *testing.T) {
	service := cascade.NewRegistryService(cascade.NewInMemoryStore())
	server := New(service)

	t.Run("GET /v2/ should return 200", func(t *testing.T) {
		request, _ := http.NewRequest(http.MethodGet, "/v2/", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
	})
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("got error where none was expected: %v", err)
		t.FailNow()
	}
}

func assertErrorInResponseBody(t *testing.T, body *bytes.Buffer, want cascade.Error) {
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
