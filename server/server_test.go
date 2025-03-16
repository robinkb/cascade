package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestRoot(t *testing.T) {
	server := newTestServer()

	t.Run("GET /v2/ should return 200", func(t *testing.T) {
		request, _ := http.NewRequest(http.MethodGet, "/v2/", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusOK)
	})
}

func newTestServer() *server.Server {
	return server.New(
		cascade.NewRegistryService(
			inmemory.NewMetadataStore(),
			inmemory.NewBlobStore(),
		),
	)
}
