package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestListReferrers(t *testing.T) {
	t.Run("Listing referrers returns 200 OK", func(t *testing.T) {
		wantName := RandomName()
		wantDigest := RandomDigest()
		wantIndex := &v1.Index{}

		server := New(&StubRegistryService{listReferrers: func(repository string, digest string) (*v1.Index, error) {
			if repository == wantName && digest == wantDigest.String() {
				return wantIndex, nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/referrers/%s", wantName, wantDigest), nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusOK)
		AssertResponseHeader(t, response.Result(), headerContentType, v1.MediaTypeImageIndex)

		var gotIndex v1.Index
		AssertResponseBodyUnmarshals(t, response.Result(), &gotIndex)
	})

	t.Run("Listing referrers with invalid digest returns 400 Bad Request", func(t *testing.T) {
		wantName := randomName()
		wantDigest := "invalid"
		wantErr := cascade.ErrDigestInvalid

		server := New(&StubRegistryService{listReferrers: func(repository, digest string) (*v1.Index, error) {
			if repository == wantName && digest == wantDigest {
				return nil, wantErr
			}
			panic(errDataNotPassedCorrectly)
		}})

		request, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/referrers/%s", wantName, wantDigest), nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusBadRequest)
	})
}
