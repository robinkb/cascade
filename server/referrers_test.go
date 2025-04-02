package server_test

import (
	"net/http"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/robinkb/cascade-registry/testing/mock"
)

func TestListReferrers(t *testing.T) {
	t.Run("Listing referrers returns 200 OK", func(t *testing.T) {
		wantName := RandomName()
		wantDigest := RandomDigest()
		wantIndex := &v1.Index{}

		service := mock.NewRegistryService(t)
		service.EXPECT().
			ListReferrers(wantName, wantDigest).
			Return(wantIndex, nil)

		client := NewTestClientWithServer(t, service)

		resp := client.ListReferrers(wantName, wantDigest)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentType, v1.MediaTypeImageIndex)

		var gotIndex v1.Index
		AssertResponseBodyUnmarshals(t, resp, &gotIndex)
	})

	t.Run("Listing referrers with invalid digest returns 400 Bad Request", func(t *testing.T) {
		wantName := RandomName()
		wantDigest := "invalid"
		wantErr := cascade.ErrDigestInvalid

		service := mock.NewRegistryService(t)
		service.EXPECT().
			ListReferrers(wantName, wantDigest).
			Return(nil, wantErr)

		client := NewTestClientWithServer(t, service)

		resp := client.ListReferrers(wantName, digest.Digest(wantDigest))

		AssertResponseCode(t, resp, http.StatusBadRequest)
	})
}
