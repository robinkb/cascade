package conformance

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestPull(t *testing.T) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)
	server := server.New(service)

	ts := httptest.NewServer(server)
	defer ts.Close()

	t.Run("Pulling manifests", func(t *testing.T) {
		t.Run("GET request to a known manifest", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			name := RandomName()
			digest, manifest, content := RandomManifest()
			metadata.PutManifest(name, digest, &cascade.ManifestMetadata{
				Path:      digest.String(),
				MediaType: manifest.MediaType,
			})
			blobs.Put(digest.String(), content)

			resp := client.GetManifestByDigest(name, digest)

			// A GET request to an existing manifest URL MUST provide the expected manifest, with a response code that MUST be 200 OK.
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBodyEquals(t, resp, content)

			// In a successful response, the Content-Type header will indicate the type of the returned manifest.
			// The registry SHOULD NOT include parameters on the Content-Type header.
			// The Content-Type header SHOULD match what the client pushed as the manifest's Content-Type.
			AssertResponseHeader(t, resp, "Content-Type", manifest.MediaType)
		})

		t.Run("GET request to an unknown manifest", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			name, digest := RandomName(), RandomDigest()
			resp := client.GetManifestByDigest(name, digest)

			// If the manifest is not found in the repository, the response code MUST be 404 Not Found.
			AssertResponseCode(t, resp, http.StatusNotFound)
		})
	})

	t.Run("Pulling blobs", func(t *testing.T) {
		name := RandomName()
		digest, blob := RandomBlob(32)

		metadata.PutBlob(name, digest, digest.String())
		blobs.Put(digest.String(), blob)

		t.Run("GET request to an existing blob", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)
			resp := client.GetBlob(name, digest)

			// A GET request to an existing blob URL MUST provide the expected blob, with a response code that MUST be 200 OK.
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBodyEquals(t, resp, blob)
		})

		t.Run("GET request to an unknown blob", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			name, digest := RandomName(), RandomDigest()
			resp := client.GetBlob(name, digest)

			// If the blob is not found in the repository, the response code MUST be 404 Not Found.
			AssertResponseCode(t, resp, http.StatusNotFound)
		})
	})

	t.Run("Checking if content exists in the registry", func(t *testing.T) {
		t.Run("HEAD request to an existing blob", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			name := RandomName()
			digest, blob := RandomBlob(32)

			resp := client.InitUpload(name)
			AssertResponseCode(t, resp, http.StatusAccepted)
			location, err := resp.Location()
			RequireNoError(t, err)
			resp = client.CloseUploadWithContent(location, digest, blob, 0)

			resp = client.CheckBlob(name, digest)

			// A HEAD request to an existing blob URL MUST return 200 OK.
			AssertResponseCode(t, resp, http.StatusOK)
			// A successful response SHOULD contain the size in bytes of the uploaded blob in the header Content-Length.
			AssertResponseHeader(t, resp, "Content-Length", strconv.Itoa(len(blob)))
		})

		t.Run("HEAD request to an unknown blob", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			name, digest := RandomName(), RandomDigest()
			resp := client.CheckBlob(name, digest)

			// If the blob is not found in the repository, the response code MUST be 404 Not Found.
			AssertResponseCode(t, resp, http.StatusNotFound)
		})

		t.Run("HEAD request to an existing manifest", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			name := RandomName()
			digest, _, content := RandomManifest()

			resp := client.PutManifest(name, digest.String(), content)
			AssertResponseCode(t, resp, http.StatusCreated)

			resp = client.CheckManifestByDigest(name, digest)

			// A HEAD request to an existing manifest URL MUST return 200 OK
			AssertResponseCode(t, resp, http.StatusOK)

			// A successful response SHOULD contain the size in bytes of the uploaded blob in the header Content-Length.
			contentLength := strconv.Itoa(len(content))
			AssertResponseHeader(t, resp, "Content-Length", contentLength)
		})

		t.Run("HEAD request to an unknown manifest", func(t *testing.T) {
			client := NewTestClient(t, ts.URL)

			name, digest := RandomName(), RandomDigest()

			resp := client.CheckManifestByDigest(name, digest)

			// If the manifest is not found in the repository, the response code MUST be 404 Not Found.
			AssertResponseCode(t, resp, http.StatusNotFound)
		})
	})
}
