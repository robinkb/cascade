package conformance

import (
	"net/http"
	"slices"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade/registry"
	v2 "github.com/robinkb/cascade/registry/api/v2"
	"github.com/robinkb/cascade/registry/store/inmemory"
	. "github.com/robinkb/cascade/testing"
	testclient "github.com/robinkb/cascade/testing/client"
)

func TestContentDiscovery(t *testing.T) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := registry.NewService(metadata, blobs)
	srv := v2.New(service)

	t.Run("Listing Tags", func(t *testing.T) {
		repository := RandomName()
		_, _, content := RandomManifest()
		tags := RandomTags(50)

		client := testclient.NewTestClientForHandler(t, srv)
		for _, tag := range tags {
			resp := client.PutManifest(repository, tag, content)
			AssertResponseCode(t, resp, http.StatusCreated)
		}

		// Sort the tags _after_ putting them into the registry,
		// to make sure that the registry is sorting them internally.
		slices.Sort(tags)

		t.Run("Fetching the whole list of tags", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			resp := client.ListTags(repository, nil)
			// Assuming a repository is found, this request MUST return a 200 OK response code.
			AssertResponseCode(t, resp, http.StatusOK)
			// Upon success, the response MUST be a json body.
			var tagsList v2.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			//  If the list is not empty, the tags MUST be in lexical order (i.e. case-insensitive alphanumeric order).
			AssertSlicesEqual(t, tags, tagsList.Tags)

			resp = client.ListTags(RandomName(), nil)
			// Assuming a repository is found, this request MUST return a 200 OK response code.
			AssertResponseCode(t, resp, http.StatusOK)
			// Upon success, the response MUST be a json body.
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// The list of tags MAY be empty if there are no tags on the repository.
			AssertSlicesEqual(t, []string{}, tagsList.Tags)
		})

		t.Run("Fetching a subset of tags", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			// In addition to fetching the whole list of tags, a subset of the tags can be fetched by providing the n query parameter.
			resp := client.ListTags(repository, &testclient.ListTagsOptions{
				N: testclient.Pointer(10),
			})
			AssertResponseCode(t, resp, http.StatusOK)
			var tagsList v2.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// Without the last query parameter (described next), the list returned will start at the beginning of the list and include <int> results.
			// The tags MUST be in lexical order.
			AssertSlicesEqual(t, tags[0:10], tagsList.Tags)
		})

		t.Run("Fetching more tags than are available", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			// The response to such a request MAY return fewer than <int> results, but only when the total number of tags attached to the repository is less than <int> or a Link header is provided.
			resp := client.ListTags(repository, &testclient.ListTagsOptions{
				N: testclient.Pointer(len(tags) + 10),
			})
			AssertResponseCode(t, resp, http.StatusOK)
			var tagsList v2.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// Otherwise, the response MUST include <int> results.
			AssertSlicesEqual(t, tags, tagsList.Tags)
		})

		t.Run("Fetching 0 tags must return an empty list", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			resp := client.ListTags(repository, &testclient.ListTagsOptions{
				N: testclient.Pointer(0),
			})

			AssertResponseCode(t, resp, http.StatusOK)
			var tagsList v2.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// When n is zero, this endpoint MUST return an empty list,
			AssertSlicesEqual(t, []string{}, tagsList.Tags)
			// and MUST NOT include a Link header.
			AssertResponseHeaderUnset(t, resp, "Link")
		})

		t.Run("Fetch tags with the 'last' query parameter", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			n := 10
			lastIndex := 10
			lastTag := tags[lastIndex]

			resp := client.ListTags(repository, &testclient.ListTagsOptions{
				Last: lastTag,
				N:    testclient.Pointer(n),
			})

			AssertResponseCode(t, resp, http.StatusOK)
			var tagsList v2.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// A list tags request including the last query parameter will return up to tags, beginning non-inclusively with <last>.
			// That is to say, will not be included in the results, but up to <n> tags after <last> will be returned.
			AssertSlicesEqual(t, tags[lastIndex+1:lastIndex+1+n], tagsList.Tags)

			lastIndex = 20
			lastTag = tags[lastIndex]

			// When using the last query parameter, the n parameter is OPTIONAL.
			resp = client.ListTags(repository, &testclient.ListTagsOptions{
				Last: lastTag,
			})
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			AssertSlicesEqual(t, tags[lastIndex+1:], tagsList.Tags)
		})
	})

	t.Run("Listing Referrers", func(t *testing.T) {
		repository := RandomName()

		subjectDigest, _, subjectContent := RandomManifest()

		wantIndex, referrers := GenerateReferrersWithIndex(t, subjectDigest)
		wantFilteredIndex := v1.Index{
			Manifests: []v1.Descriptor{
				wantIndex.Manifests[0],
			},
		}

		client := testclient.NewTestClientForHandler(t, srv)

		resp := client.PutManifest(repository, subjectDigest.String(), subjectContent)
		AssertResponseCode(t, resp, http.StatusCreated)

		for _, referrer := range referrers {
			resp = client.PutManifest(repository, referrer.Digest.String(), referrer.Content)
			AssertResponseCode(t, resp, http.StatusCreated)
		}

		t.Run("Fetching the full list of referrers", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			resp := client.ListReferrers(repository, subjectDigest, nil)
			// Assuming a repository is found, this request MUST return a 200 OK response code.
			AssertResponseCode(t, resp, http.StatusOK)
			// The Content-Type header MUST be set to application/vnd.oci.image.index.v1+json.
			AssertResponseHeader(t, resp, "Content-Type", "application/vnd.oci.image.index.v1+json")
			// Upon success, the response MUST be a JSON body with an image gotIndex containing a list of descriptors.
			var gotIndex v1.Index
			AssertResponseBodyUnmarshals(t, resp, &gotIndex)

			// Each descriptor is of an image manifest or index in the same <name> namespace
			// with a subject field that specifies the value of <digest>.
			AssertIndex(t, &gotIndex, wantIndex)
		})

		t.Run("Fetching a filtered list of referrers", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			// The registry SHOULD support filtering on artifactType.
			resp := client.ListReferrers(repository, subjectDigest, &testclient.ListReferrersOptions{
				ArtifactType: "application/vnd.example+type",
			})

			// Assuming a repository is found, this request MUST return a 200 OK response code.
			AssertResponseCode(t, resp, http.StatusOK)
			// The Content-Type header MUST be set to application/vnd.oci.image.index.v1+json.
			AssertResponseHeader(t, resp, "Content-Type", "application/vnd.oci.image.index.v1+json")
			//  If filtering is requested and applied, the response MUST include a header
			// OCI-Filters-Applied: artifactType denoting that an artifactType filter was applied.
			AssertResponseHeader(t, resp, "OCI-Filters-Applied", "artifactType")

			// Upon success, the response MUST be a JSON body with an image gotIndex containing a list of descriptors.
			var gotIndex v1.Index
			AssertResponseBodyUnmarshals(t, resp, &gotIndex)

			// Each descriptor is of an image manifest or index in the same <name> namespace
			// with a subject field that specifies the value of <digest>.
			AssertIndex(t, &gotIndex, &wantFilteredIndex)
		})

		t.Run("List referrers on existing repository without referrers", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			digest := RandomDigest()

			resp := client.ListReferrers(repository, digest, nil)
			// If the registry supports the referrers API, the registry MUST NOT return a 404 Not Found to a referrers API requests.
			AssertResponseCode(t, resp, http.StatusOK)
		})

		t.Run("List referrers with an invalid request", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			resp := client.ListReferrers(repository, "12345", nil)
			// If the request is invalid, such as a <digest> with an invalid syntax, a 400 Bad Request MUST be returned.
			AssertResponseCode(t, resp, http.StatusBadRequest)
		})
	})
}
