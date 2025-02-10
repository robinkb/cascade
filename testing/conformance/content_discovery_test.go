package conformance

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestContentDiscovery(t *testing.T) {
	metadata := cascade.NewInMemoryMetadataStore()
	blobs := cascade.NewInMemoryBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)
	srv := server.New(service)

	ts := httptest.NewServer(srv)
	defer ts.Close()

	t.Run("Listing Tags", func(t *testing.T) {
		repository := RandomName()
		digest := RandomDigest()

		tags := make([]string, 50)
		for i := 0; i < len(tags); i++ {
			tags[i] = RandomVersion()
		}

		for _, tag := range tags {
			metadata.PutTag(repository, tag, digest.String())
		}

		// Sort the tags _after_ putting them into the registry,
		// to make sure that the registry is sorting them internally.
		slices.Sort(tags)

		t.Run("Fetching the whole list of tags", func(t *testing.T) {
			client := NewClient(t, ts.URL)

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
			client := NewClient(t, ts.URL)

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
			client := NewClient(t, ts.URL)

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
			client := NewClient(t, ts.URL)

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
			client := NewClient(t, ts.URL)

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

	t.Run("Listing Referrers", func(t *testing.T) {
		client := NewClient(t, ts.URL)

		repository, digest, manifest := RandomManifest()

		resp := client.PutManifest(repository, digest.String(), manifest)
		AssertResponseCode(t, resp, http.StatusCreated)

		referrerCount := 3
		referrers := make([]v1.Descriptor, referrerCount)
		for i := 0; i < referrerCount; i++ {
			manifest, digest := RandomManifestWithSubject(manifest, digest)
			referrers[i] = v1.Descriptor{
				MediaType:    manifest.MediaType,
				ArtifactType: manifest.ArtifactType,
				Digest:       digest,
				Size:         int64(len(manifest.Bytes())),
				Annotations:  manifest.Annotations,
			}
			resp = client.PutManifest(repository, digest.String(), manifest)
			AssertResponseCode(t, resp, http.StatusCreated)
		}

		t.Run("List referrers on existing repository", func(t *testing.T) {
			resp = client.ListReferrers(repository, digest)
			// Assuming a repository is found, this request MUST return a 200 OK response code.
			AssertResponseCode(t, resp, http.StatusOK)
			// The Content-Type header MUST be set to application/vnd.oci.image.index.v1+json.
			AssertResponseHeader(t, resp, "Content-Type", "application/vnd.oci.image.index.v1+json")

			// Upon success, the response MUST be a JSON body with an image index containing a list of descriptors.
			var index v1.Index
			AssertResponseBodyUnmarshals(t, resp, &index)

			AssertIndexContainsReferrers(t, &index, referrers...)
			// TODO: Assert index contents
		})

		t.Run("List referrers on unknown repository", func(t *testing.T) {
			repository := RandomName()
			digest := RandomDigest()

			resp := client.ListReferrers(repository, digest)
			// If the registry supports the referrers API, the registry MUST NOT return a 404 Not Found to a referrers API requests.
			AssertResponseCode(t, resp, http.StatusOK)
		})

		t.Run("List referrers with a bad digest", func(t *testing.T) {
			resp := client.ListReferrers(repository, "12345")
			// If the request is invalid, such as a <digest> with an invalid syntax, a 400 Bad Request MUST be returned.
			AssertResponseCode(t, resp, http.StatusBadRequest)
		})

	})
}
