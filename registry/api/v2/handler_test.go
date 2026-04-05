package v2

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/robinkb/cascade/registry"
	"github.com/robinkb/cascade/registry/repository"
	"github.com/robinkb/cascade/registry/store/driver/inmemory"
	. "github.com/robinkb/cascade/testing"
	testclient "github.com/robinkb/cascade/testing/client"
	mock "github.com/robinkb/cascade/testing/mock/registry"
)

func TestRoot(t *testing.T) {
	server := New(
		registry.New(
			inmemory.NewMetadataStore(),
			inmemory.NewBlobStore(),
		),
	)

	t.Run("GET /v2/ should return 200", func(t *testing.T) {
		request, _ := http.NewRequest(http.MethodGet, "/v2/", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusOK)
	})
}

func newLocation(name, sessionID string) *url.URL {
	return &url.URL{Path: Location(name, sessionID)}
}

// NewTestClientForRepository wraps around NewTestClientForHandler to provide a test client
// for a registry that only returns the given RepositoryService under the specified name.
// Attempting to create, read, update, or delete objects in any other repository will panic.
func NewTestClientForRepository(t *testing.T, name string, service repository.Service) *testclient.Client {
	registry := mock.NewService(t)
	registry.EXPECT().
		GetRepository(name).
		Return(service, nil)

	return testclient.NewForHandler(t, New(registry))
}

func AssertResponseBodyContainsError(t *testing.T, got *http.Response, want repository.Error) *Result {
	t.Helper()

	if got.Body == nil || got.Body == http.NoBody {
		t.Errorf("response body is empty while expecting error")
		return &Result{T: t, Success: false}
	}

	var errs ErrorResponse
	err := json.NewDecoder(got.Body).Decode(&errs)
	RequireNoError(t, err)

	for _, err := range errs.Errors {
		if errors.Is(err, want) {
			return &Result{T: t, Success: true}
		}
	}

	t.Errorf("could not find error in response body; got %q, want %q", errs, want)
	return &Result{T: t, Success: false}
}
