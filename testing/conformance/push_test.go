package conformance

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestPush(t *testing.T) {
	metadata := cascade.NewInMemoryMetadataStore()
	blobs := cascade.NewInMemoryBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)
	server := server.New(service)

	ts := httptest.NewServer(server)
	defer ts.Close()

	t.Run("Pushing blobs", func(t *testing.T) {
		t.Run("Pushing a blob monolithically", func(t *testing.T) {
			t.Run("POST then PUT", func(t *testing.T) {
				client := NewClient(t, ts.URL)

				name, digest, blob := RandomBlob(32)
				resp := client.InitUpload(name)

				// When obtaining a session ID, the response MUST have a code of 202 Accepted.
				AssertResponseCode(t, resp, http.StatusAccepted)

				// The <location> MUST contain a UUID representing a unique session ID for the upload to follow.
				location, err := resp.Location()
				RequireNoError(t, err)

				resp = client.UploadBlobMonolithic(location.String(), digest, blob)

				// Upon successful completion of the request, the response MUST have code 201 Created.
				AssertResponseCode(t, resp, http.StatusCreated)

				// The Location header MUST be a pullable blob URL.
				location, err = resp.Location()
				RequireNoError(t, err)

				resp = client.Do(http.MethodGet, location.RequestURI(), nil, nil)

				AssertResponseCode(t, resp, http.StatusOK)
				AssertResponseBody(t, resp, blob)
			})

			t.Run("Single POST", func(t *testing.T) {
				client := NewClient(t, ts.URL)

				// Registries MAY support pushing blobs using a single POST request.
				// Cascade does not.
				name, digest, blob := RandomBlob(32)
				resp := client.UploadBlobSinglePOST(name, digest, blob)

				// Registries that do not support single request monolithic uploads
				// SHOULD return a 202 Accepted status code and Location header.
				AssertResponseCode(t, resp, http.StatusAccepted)
				_, err := resp.Location()
				AssertNoError(t, err)
			})
		})
	})
}
