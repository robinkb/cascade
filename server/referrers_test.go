package server_test

import (
	"errors"
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
	wantName := RandomName()
	wantDigest := RandomDigest()

	t.Run("Listing referrers returns 200 OK", func(t *testing.T) {
		wantIndex, _ := GenerateReferrersWithIndex(t, RandomDigest())
		wantReferrers := cascade.Referrers{
			Index: wantIndex,
		}

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			ListReferrers(wantName, wantDigest.String(), mock.Anything).
			Return(&wantReferrers, nil)

		client := NewTestClientWithRepository(t, wantName, repository)

		resp := client.ListReferrers(wantName, wantDigest, nil)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderContentType, v1.MediaTypeImageIndex)
		AssertResponseHeaderUnset(t, resp, server.HeaderOCIFiltersApplied)

		var gotIndex v1.Index
		AssertResponseBodyUnmarshals(t, resp, &gotIndex)
		AssertIndex(t, &gotIndex, wantIndex)
	})

	t.Run("Listing referrers with filter parses the query option and sets a header", func(t *testing.T) {
		wantArtifactType := RandomString(12)
		wantOpts := cascade.ListReferrersOptions{
			ArtifactType: wantArtifactType,
		}
		wantReferrers := cascade.Referrers{
			AppliedFilters: []string{"artifactType"},
		}

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			ListReferrers(wantName, wantDigest.String(), &wantOpts).
			Return(&wantReferrers, nil)

		client := NewTestClientWithRepository(t, wantName, repository)

		resp := client.ListReferrers(wantName, wantDigest, &ListReferrersOptions{
			ArtifactType: wantArtifactType,
		})

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, server.HeaderOCIFiltersApplied, "artifactType")

		var gotIndex v1.Index
		AssertResponseBodyUnmarshals(t, resp, &gotIndex)
	})

	t.Run("Listing referrers with invalid digest returns 400 Bad Request", func(t *testing.T) {
		wantDigest := "invalid"
		wantErr := cascade.ErrDigestInvalid

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			ListReferrers(wantName, wantDigest, mock.Anything).
			Return(nil, wantErr)

		client := NewTestClientWithRepository(t, wantName, repository)

		resp := client.ListReferrers(wantName, digest.Digest(wantDigest), nil)

		AssertResponseCode(t, resp, http.StatusBadRequest)
	})

	t.Run("Unknown service errors return a 500 internal server error", func(t *testing.T) {
		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			ListReferrers(wantName, wantDigest.String(), mock.Anything).
			Return(nil, errors.New("unknown"))

		client := NewTestClientWithRepository(t, wantName, repository)

		resp := client.ListReferrers(wantName, wantDigest, nil)

		AssertResponseCode(t, resp, http.StatusInternalServerError)
	})
}
