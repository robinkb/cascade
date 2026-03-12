package conformance

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/robinkb/cascade-registry/registry"
	v2 "github.com/robinkb/cascade-registry/registry/api/v2"
	"github.com/robinkb/cascade-registry/registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
	testclient "github.com/robinkb/cascade-registry/testing/client"
)

func TestPush(t *testing.T) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := registry.NewService(metadata, blobs)
	srv := v2.New(service)

	t.Run("Pushing blobs", func(t *testing.T) {
		t.Run("Pushing a blob monolithically", func(t *testing.T) {
			t.Run("POST then PUT", func(t *testing.T) {
				client := testclient.NewTestClientForHandler(t, srv)

				name := RandomName()
				digest, blob := RandomBlob(32)

				// When obtaining a session ID, the response MUST have a code of 202 Accepted.
				resp := client.InitUpload(name)
				AssertResponseCode(t, resp, http.StatusAccepted)

				// The <location> MUST contain a UUID representing a unique session ID for the upload to follow.
				location, err := resp.Location()
				RequireNoError(t, err)

				// Close the session with a PUT request
				resp = client.CloseUploadWithContent(location, digest, blob, 0)
				// Upon successful completion of the request, the response MUST have code 201 Created.
				AssertResponseCode(t, resp, http.StatusCreated)
				location, err = resp.Location()
				RequireNoError(t, err)

				// The Location header MUST be a pullable blob URL.
				resp = client.Do(http.MethodGet, location.RequestURI(), nil, nil)
				AssertResponseCode(t, resp, http.StatusOK)
				AssertResponseBodyEquals(t, resp, blob)
			})

			t.Run("Single POST", func(t *testing.T) {
				client := testclient.NewTestClientForHandler(t, srv)

				// Registries MAY support pushing blobs using a single POST request.
				// Cascade does not.
				name := RandomName()
				digest, blob := RandomBlob(32)
				resp := client.UploadBlobSinglePOST(name, digest, blob)

				// Registries that do not support single request monolithic uploads
				// SHOULD return a 202 Accepted status code and Location header.
				AssertResponseCode(t, resp, http.StatusAccepted)
				_, err := resp.Location()
				AssertNoError(t, err)
			})
		})

		t.Run("Pushing a blob in chunks", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			name := RandomName()
			digest, blob := RandomBlob(64 * 1024)

			// Obtain a session ID
			resp := client.InitUpload(name)
			AssertResponseCode(t, resp, http.StatusAccepted)
			location, err := resp.Location()
			RequireNoError(t, err)

			// The first chunk's range MUST begin with 0.
			resp = client.UploadBlobChunk(location, []byte{}, 10)
			AssertResponseCode(t, resp, http.StatusRequestedRangeNotSatisfiable)

			// The Content-Range header MUST match the following regular expression: ^[0-9]+-[0-9]+$
			resp = client.Do(http.MethodPatch, location.RequestURI(), http.Header{
				"Content-Type":   []string{"application/octet-stream"},
				"Content-Range":  []string{"a-b"},
				"Content-Length": []string{"10"},
			}, nil)
			AssertResponseCode(t, resp, http.StatusBadRequest)

			r := bytes.NewReader(blob)
			buffer := make([]byte, 16*1024)
			written := 0

			for {
				n, err := io.ReadFull(r, buffer)
				RequireNoError(t, err)

				resp := client.UploadBlobChunk(location, buffer, written)

				written += n

				// Each successful chunk upload MUST have a 202 Accepted response code,
				AssertResponseCode(t, resp, http.StatusAccepted)
				// and MUST have the following headers:
				location, err = resp.Location()
				RequireNoError(t, err)
				AssertResponseHeader(t, resp, "Range", fmt.Sprintf("0-%d", written-1))

				if r.Len() == 0 {
					break
				}
			}

			// The response to an active upload MUST be a 204 No Content response code,
			resp = client.CheckUpload(location)
			AssertResponseCode(t, resp, http.StatusNoContent)
			//  and MUST have the following headers:
			location, err = resp.Location()
			AssertNoError(t, err)
			AssertResponseHeader(t, resp, "Range", fmt.Sprintf("0-%d", written-1))

			// If a chunk is uploaded out of order, the registry MUST respond
			// with a 416 Requested Range Not Satisfiable code.
			resp = client.UploadBlobChunk(location, []byte{}, 256)
			AssertResponseCode(t, resp, http.StatusRequestedRangeNotSatisfiable)

			resp = client.CloseUpload(location, digest)
			// The response to a successful closing of the session MUST be 201 Created
			AssertResponseCode(t, resp, http.StatusCreated)
			// and MUST have the following header:
			location, err = resp.Location()
			RequireNoError(t, err)

			// The Location header MUST be a pullable blob URL.
			resp = client.Do(http.MethodGet, location.RequestURI(), nil, nil)
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBodyEquals(t, resp, blob)
		})

		// This is not part of the spec (yet).
		t.Run("Pushing a blob as a stream", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			name := RandomName()
			digest, blob := RandomBlob(64 * 1024)

			resp := client.InitUpload(name)
			AssertResponseCode(t, resp, http.StatusAccepted)
			location, err := resp.Location()
			RequireNoError(t, err)

			resp = client.UploadBlobStream(location, bytes.NewBuffer(blob))
			AssertResponseCode(t, resp, http.StatusAccepted)

			resp = client.CloseUpload(location, digest)
			AssertResponseCode(t, resp, http.StatusCreated)
			location, err = resp.Location()
			RequireNoError(t, err)

			resp = client.Do(http.MethodGet, location.RequestURI(), nil, nil)
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBodyEquals(t, resp, blob)
		})
	})

	t.Run("Pushing manifests", func(t *testing.T) {
		t.Run("Pushing manifest by digest", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			name := RandomName()
			digest, _, content := RandomManifest()

			// Upon a successful upload, the registry MUST return response code 201 Created,
			resp := client.PutManifest(name, digest.String(), content)
			AssertResponseCode(t, resp, http.StatusCreated)
			// and MUST have the following header:
			location, err := resp.Location()
			AssertNoError(t, err)

			resp = client.Do(http.MethodGet, location.RequestURI(), nil, nil)
			AssertResponseCode(t, resp, http.StatusOK)
			// The registry MUST store the manifest in the exact byte representation provided by the client.
			AssertResponseBodyEquals(t, resp, content)
		})

		t.Run("Pushing manifest by tag", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			name := RandomName()
			digest, _, content := RandomManifest()
			tag := "40"

			// Upon a successful upload, the registry MUST return response code 201 Created,
			resp := client.PutManifest(name, tag, content)
			AssertResponseCode(t, resp, http.StatusCreated)
			// and MUST have the following header:
			location, err := resp.Location()
			AssertNoError(t, err)

			resp = client.Do(http.MethodGet, location.RequestURI(), nil, nil)
			AssertResponseCode(t, resp, http.StatusOK)
			// The registry MUST store the manifest in the exact byte representation provided by the client.
			AssertResponseBodyEquals(t, resp, content)

			resp = client.GetManifestByDigest(name, digest)
			AssertResponseCode(t, resp, http.StatusOK)
			AssertResponseBodyEquals(t, resp, content)
		})

		t.Run("Pushing manifest with layers", func(t *testing.T) {})

		t.Run("Pushing manifests with Subject", func(t *testing.T) {
			client := testclient.NewTestClientForHandler(t, srv)

			name := RandomName()
			subjectDigest, subjectManifest, _ := RandomManifest()
			digest, _, content := RandomManifestWithSubject(subjectDigest, subjectManifest)

			resp := client.PutManifest(name, digest.String(), content)

			// When processing a request for an image manifest with the subject field,
			// a registry implementation that supports the referrers API MUST respond with the
			// response header OCI-Subject: <subject digest> to indicate to the client
			// that the registry processed the request's subject.
			// NOTE: The subject may not exist in the registry.
			AssertResponseCode(t, resp, http.StatusCreated)
			AssertResponseHeader(t, resp, "OCI-Subject", subjectDigest.String())
		})
	})
}
