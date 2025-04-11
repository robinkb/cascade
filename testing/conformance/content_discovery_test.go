package conformance

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestContentDiscovery(t *testing.T) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)
	srv := server.New(service)

	ts := httptest.NewServer(srv)
	defer ts.Close()

	repository := RandomName()
	digest := RandomDigest()
	tags := make([]string, 50)
	for i := 0; i < len(tags); i++ {
		tags[i] = RandomVersion()
	}

	for _, tag := range tags {
		metadata.PutTag(repository, tag, digest)
	}

	// Sort the tags _after_ putting them into the registry,
	// to make sure that the registry is sorting them internally.
	slices.Sort(tags)

	t.Run("Listing Tags", func(t *testing.T) {
		t.Run("Fetching the whole list of tags", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			resp := client.ListTags(repository, nil)
			// Assuming a repository is found, this request MUST return a 200 OK response code.
			AssertResponseCode(t, resp, http.StatusOK)
			// Upon success, the response MUST be a json body.
			var tagsList server.TagsListResponse
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
			client := NewTestClient(t, ts.URL)

			// In addition to fetching the whole list of tags, a subset of the tags can be fetched by providing the n query parameter.
			resp := client.ListTags(repository, &ListTagsOptions{
				N: Pointer(10),
			})
			AssertResponseCode(t, resp, http.StatusOK)
			var tagsList server.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// Without the last query parameter (described next), the list returned will start at the beginning of the list and include <int> results.
			// The tags MUST be in lexical order.
			AssertSlicesEqual(t, tags[0:10], tagsList.Tags)
		})

		t.Run("Fetching more tags than are available", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			// The response to such a request MAY return fewer than <int> results, but only when the total number of tags attached to the repository is less than <int> or a Link header is provided.
			resp := client.ListTags(repository, &ListTagsOptions{
				N: Pointer(len(tags) + 10),
			})
			AssertResponseCode(t, resp, http.StatusOK)
			var tagsList server.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// Otherwise, the response MUST include <int> results.
			AssertSlicesEqual(t, tags, tagsList.Tags)
		})

		t.Run("Fetching 0 tags must return an empty list", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			resp := client.ListTags(repository, &ListTagsOptions{
				N: Pointer(0),
			})

			AssertResponseCode(t, resp, http.StatusOK)
			var tagsList server.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// When n is zero, this endpoint MUST return an empty list,
			AssertSlicesEqual(t, []string{}, tagsList.Tags)
			// and MUST NOT include a Link header.
			AssertResponseHeaderUnset(t, resp, "Link")
		})

		t.Run("Fetch tags with the 'last' query parameter", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			n := 10
			lastIndex := 10
			lastTag := tags[lastIndex]

			resp := client.ListTags(repository, &ListTagsOptions{
				Last: lastTag,
				N:    Pointer(n),
			})

			AssertResponseCode(t, resp, http.StatusOK)
			var tagsList server.TagsListResponse
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			// A list tags request including the last query parameter will return up to tags, beginning non-inclusively with <last>.
			// That is to say, will not be included in the results, but up to <n> tags after <last> will be returned.
			AssertSlicesEqual(t, tags[lastIndex+1:lastIndex+1+n], tagsList.Tags)

			lastIndex = 20
			lastTag = tags[lastIndex]

			// When using the last query parameter, the n parameter is OPTIONAL.
			resp = client.ListTags(repository, &ListTagsOptions{
				Last: lastTag,
			})
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBodyUnmarshals(t, resp, &tagsList)
			AssertSlicesEqual(t, tags[lastIndex+1:], tagsList.Tags)
		})
	})
}
