package conformance

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestPull(t *testing.T) {
	metadata := cascade.NewInMemoryMetadataStore()
	blobs := cascade.NewInMemoryBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)
	server := server.New(service)

	ts := httptest.NewServer(server)
	defer ts.Close()

	client := NewClient(t, ts.URL)

	t.Run("Pulling manifests", func(t *testing.T) {
		t.Run("GET request to a known manifest", func(t *testing.T) {
			name, digest, manifest := RandomManifest()
			metadata.PutManifest(name, digest, digest.String())
			blobs.Put(digest.String(), manifest.Bytes())

			resp := client.GetManifest(name, digest)

			// A GET request to an existing manifest URL MUST provide the expected manifest, with a response code that MUST be 200 OK.
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBody(t, resp, manifest.Bytes())

			// In a successful response, the Content-Type header will indicate the type of the returned manifest.
			// The registry SHOULD NOT include parameters on the Content-Type header.
			// The Content-Type header SHOULD match what the client pushed as the manifest's Content-Type.
			AssertResponseHeader(t, resp, "Content-Type", manifest.MediaType)
		})

		t.Run("GET request to an unknown manifest", func(t *testing.T) {
			name, digest := RandomName(), RandomDigest()
			resp := client.GetManifest(name, digest)

			// If the manifest is not found in the repository, the response code MUST be 404 Not Found.
			AssertResponseCode(t, resp, http.StatusNotFound)
		})
	})

	t.Run("Pulling blobs", func(t *testing.T) {
		name, digest, blob := RandomBlob(32)
		metadata.PutBlob(name, digest, digest.String())
		blobs.Put(digest.String(), blob)

		t.Run("GET request to an existing blob", func(t *testing.T) {
			resp := client.GetBlob(name, digest)

			// A GET request to an existing blob URL MUST provide the expected blob, with a response code that MUST be 200 OK.
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBody(t, resp, blob)
		})

		t.Run("GET request to an unknown blob", func(t *testing.T) {
			name, digest := RandomName(), RandomDigest()
			resp := client.GetBlob(name, digest)

			// If the blob is not found in the repository, the response code MUST be 404 Not Found.
			AssertResponseCode(t, resp, http.StatusNotFound)
		})
	})

	t.Run("Checking if content exists in the registry", func(t *testing.T) {
		t.Run("HEAD request to an existing blob", func(t *testing.T) {
			name, digest, blob := RandomBlob(32)
			metadata.PutBlob(name, digest, digest.String())
			blobs.Put(digest.String(), blob)

			resp := client.CheckBlob(name, digest)

			// A HEAD request to an existing blob URL MUST return 200 OK.
			AssertResponseCode(t, resp, http.StatusOK)
			// A successful response SHOULD contain the size in bytes of the uploaded blob in the header Content-Length.
			AssertResponseHeader(t, resp, "Content-Length", strconv.Itoa(len(blob)))
		})

		t.Run("HEAD request to an existing manifest", func(t *testing.T) {
			name, digest, manifest := RandomManifest()
			metadata.PutManifest(name, digest, digest.String())
			blobs.Put(digest.String(), manifest.Bytes())

			resp := client.CheckManifest(name, digest)

			// A HEAD request to an existing manifest URL MUST return 200 OK
			AssertResponseCode(t, resp, http.StatusOK)

			// A successful response SHOULD contain the size in bytes of the uploaded blob in the header Content-Length.
			contentLength := strconv.Itoa(len(manifest.Bytes()))
			AssertResponseHeader(t, resp, "Content-Length", contentLength)
		})
	})
}
