package server_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestRoot(t *testing.T) {
	server := server.New(
		cascade.NewRegistryService(
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
	return &url.URL{Path: fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, sessionID)}
}
