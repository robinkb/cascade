package v2

import (
	"errors"
	"net/http"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade/registry/repository"
	. "github.com/robinkb/cascade/testing"
	testclient "github.com/robinkb/cascade/testing/client"
	"github.com/robinkb/cascade/testing/mock"
)

func TestListReferrers(t *testing.T) {
	wantName := RandomName()
	wantDigest := RandomDigest()

	t.Run("Listing referrers returns 200 OK", func(t *testing.T) {
		wantIndex, _ := GenerateReferrersWithIndex(t, RandomDigest())
		wantReferrers := repository.Referrers{
			Index: wantIndex,
		}

		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			ListReferrers(wantDigest.String(), mock.Anything).
			Return(&wantReferrers, nil)

		client := NewTestClientForRepository(t, wantName, repo)

		resp := client.ListReferrers(wantName, wantDigest, nil)

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, HeaderContentType, v1.MediaTypeImageIndex)
		AssertResponseHeaderUnset(t, resp, HeaderOCIFiltersApplied)

		var gotIndex v1.Index
		AssertResponseBodyUnmarshals(t, resp, &gotIndex)
		AssertIndex(t, &gotIndex, wantIndex)
	})

	t.Run("Listing referrers with filter parses the query option and sets a header", func(t *testing.T) {
		wantArtifactType := RandomString(12)
		wantOpts := repository.ListReferrersOptions{
			ArtifactType: wantArtifactType,
		}
		wantReferrers := repository.Referrers{
			AppliedFilters: []string{"artifactType"},
		}

		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			ListReferrers(wantDigest.String(), &wantOpts).
			Return(&wantReferrers, nil)

		client := NewTestClientForRepository(t, wantName, repo)

		resp := client.ListReferrers(wantName, wantDigest, &testclient.ListReferrersOptions{
			ArtifactType: wantArtifactType,
		})

		AssertResponseCode(t, resp, http.StatusOK)
		AssertResponseHeader(t, resp, HeaderOCIFiltersApplied, "artifactType")

		var gotIndex v1.Index
		AssertResponseBodyUnmarshals(t, resp, &gotIndex)
	})

	t.Run("Listing referrers with invalid digest returns 400 Bad Request", func(t *testing.T) {
		wantDigest := "invalid"
		wantErr := repository.ErrDigestInvalid

		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			ListReferrers(wantDigest, mock.Anything).
			Return(nil, wantErr)

		client := NewTestClientForRepository(t, wantName, repo)

		resp := client.ListReferrers(wantName, digest.Digest(wantDigest), nil)

		AssertResponseCode(t, resp, http.StatusBadRequest)
	})

	t.Run("Unknown service errors return a 500 internal server error", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			ListReferrers(wantDigest.String(), mock.Anything).
			Return(nil, errors.New("unknown"))

		client := NewTestClientForRepository(t, wantName, repo)

		resp := client.ListReferrers(wantName, wantDigest, nil)

		AssertResponseCode(t, resp, http.StatusInternalServerError)
	})
}
