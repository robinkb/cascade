package conformance

import (
	"net/http"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestContentManagement(t *testing.T) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)
	srv := server.New(service)

	t.Run("Deleting tags", func(t *testing.T) {
		repository := RandomName()
		_, _, manifest := RandomManifest()
		tag := RandomVersion()

		client := NewTestClientForHandler(t, srv)

		resp := client.PutManifest(repository, tag, manifest)
		AssertResponseCode(t, resp, http.StatusCreated)

		resp = client.CheckManifestByTag(repository, tag)
		AssertResponseCode(t, resp, http.StatusOK)

		resp = client.DeleteTag(repository, tag)
		// Upon success, the registry MUST respond with a 202 Accepted code.
		AssertResponseCode(t, resp, http.StatusAccepted)

		resp = client.CheckManifestByTag(repository, tag)
		AssertResponseCode(t, resp, http.StatusNotFound)
	})

	t.Run("Deleting manifests", func(t *testing.T) {
		repository := RandomName()
		digest, _, content := RandomManifest()

		client := NewTestClientForHandler(t, srv)

		resp := client.PutManifest(repository, digest.String(), content)
		AssertResponseCode(t, resp, http.StatusCreated)

		resp = client.CheckManifestByDigest(repository, digest)
		AssertResponseCode(t, resp, http.StatusOK)

		// Upon success, the registry MUST respond with a 202 Accepted code.
		resp = client.DeleteManifest(repository, digest)
		AssertResponseCode(t, resp, http.StatusAccepted)

		resp = client.CheckManifestByDigest(repository, digest)
		AssertResponseCode(t, resp, http.StatusNotFound)

		unknownRepository := RandomName()

		resp = client.CheckManifestByDigest(unknownRepository, digest)
		// If the repository does not exist, the response MUST return 404 Not Found.
		AssertResponseCode(t, resp, http.StatusNotFound)
	})

	t.Run("Deleting blobs", func(t *testing.T) {
		name := RandomName()
		digest, content := RandomBlob(64)

		client := NewTestClientForHandler(t, srv)

		resp := client.InitUpload(name)
		AssertResponseCode(t, resp, http.StatusAccepted)

		location, err := resp.Location()
		RequireNoError(t, err)

		resp = client.CloseUploadWithContent(location, digest, content, 0)
		AssertResponseCode(t, resp, http.StatusCreated)

		resp = client.CheckBlob(name, digest)
		AssertResponseCode(t, resp, http.StatusOK)

		resp = client.DeleteBlob(name, digest)
		// Upon success, the registry MUST respond with code 202 Accepted.
		AssertResponseCode(t, resp, http.StatusAccepted)

		unknownRepository := RandomName()

		resp = client.CheckBlob(unknownRepository, digest)
		// If the blob is not found, a 404 Not Found code MUST be returned.
		AssertResponseCode(t, resp, http.StatusNotFound)
	})
}
