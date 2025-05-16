package testing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/server"
	"github.com/robinkb/cascade-registry/testing/mock"
)

// NewTestClientForHandler returns a test client for the given handler, likely a registry server.
func NewTestClientForHandler(t *testing.T, handler http.Handler) *client {
	return &client{
		t:       t,
		handler: handler,
	}
}

// NewTestClientForRepository wraps around NewTestClientForHandler to provide a test client
// for a registry that only returns the given RepositoryService under the specified name.
// Attempting to create, read, update, or delete objects in any other repository will panic.
func NewTestClientForRepository(t *testing.T, name string, service cascade.RepositoryService) *client {
	registry := mock.NewRegistryService(t)
	registry.EXPECT().
		GetRepository(name).
		Return(service, nil)

	return NewTestClientForHandler(t, server.New(registry))
}

// client is a simple registry client meant for use in testing.
// It will automatically fail tests if it encounters an unexpected error.
// Users should initialize it with NewClient().
type client struct {
	handler http.Handler
	t       *testing.T
}

func (c *client) CheckBlob(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
	return c.Do(http.MethodHead, path, nil, nil)
}

func (c *client) GetBlob(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
	return c.Do(http.MethodGet, path, nil, nil)
}

func (c *client) DeleteBlob(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
	return c.Do(http.MethodDelete, path, nil, nil)
}

func (c *client) CheckManifestByDigest(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, digest)
	return c.Do(http.MethodHead, path, nil, nil)
}

func (c *client) CheckManifestByTag(name string, tag string) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, tag)
	return c.Do(http.MethodHead, path, nil, nil)
}

func (c *client) GetManifestByDigest(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, digest)
	return c.Do(http.MethodGet, path, nil, nil)
}

func (c *client) GetManifestByTag(name string, tag string) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, tag)
	return c.Do(http.MethodGet, path, nil, nil)
}

func (c *client) PutManifest(name string, reference string, content []byte) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, reference)

	var manifest v1.Manifest
	err := json.Unmarshal(content, &manifest)
	if err != nil {
		c.t.Fatal("failed to unmarshal manifest")
	}

	headers := make(http.Header)
	headers.Set("Content-Type", manifest.MediaType)

	return c.Do(http.MethodPut, path, headers, bytes.NewBuffer(content))
}

func (c *client) DeleteManifest(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, digest)
	return c.Do(http.MethodDelete, path, nil, nil)
}

type ListTagsOptions struct {
	N    *int
	Last string
}

func (c *client) ListTags(name string, opts *ListTagsOptions) *http.Response {
	u := url.URL{
		Path: fmt.Sprintf("/v2/%s/tags/list", name),
	}

	if opts != nil {
		query := make(url.Values, 0)
		if opts.N != nil {
			query.Set("n", strconv.Itoa(*opts.N))
		}
		if opts.Last != "" {
			query.Set("last", opts.Last)
		}
		u.RawQuery = query.Encode()
	}

	return c.Do(http.MethodGet, u.RequestURI(), nil, nil)
}

func (c *client) DeleteTag(name string, tag string) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, tag)
	return c.Do(http.MethodDelete, path, nil, nil)
}

type ListReferrersOptions struct {
	ArtifactType string
}

func (c *client) ListReferrers(name string, digest digest.Digest, opts *ListReferrersOptions) *http.Response {
	u := url.URL{
		Path: fmt.Sprintf("/v2/%s/referrers/%s", name, digest),
	}

	if opts != nil {
		query := make(url.Values, 0)
		if opts.ArtifactType != "" {
			query.Set("artifactType", opts.ArtifactType)
		}
		u.RawQuery = query.Encode()
	}
	return c.Do(http.MethodGet, u.RequestURI(), nil, nil)
}

func (c *client) InitUpload(name string) *http.Response {
	path := fmt.Sprintf("/v2/%s/blobs/uploads/", name)
	return c.Do(http.MethodPost, path, nil, nil)
}

func (c *client) CheckUpload(location *url.URL) *http.Response {
	return c.Do(http.MethodGet, location.RequestURI(), nil, nil)
}

func (c *client) UploadBlobSinglePOST(name string, digest digest.Digest, content []byte) *http.Response {
	path := fmt.Sprintf("/v2/%s/blobs/uploads/", name)

	url, err := url.Parse(path)
	RequireNoError(c.t, err)

	query := url.Query()
	query.Add("digest", digest.String())
	url.RawQuery = query.Encode()

	headers := make(http.Header)
	headers.Set("Content-Type", "application/octet-stream")
	headers.Set("Content-length", strconv.Itoa(len(content)))

	return c.Do(http.MethodPost, path, headers, bytes.NewBuffer(content))
}

// CloseUpload performs a PUT to the upload location to close the upload session.
func (c *client) CloseUpload(location *url.URL, digest digest.Digest) *http.Response {
	query := location.Query()
	query.Add("digest", digest.String())
	location.RawQuery = query.Encode()

	return c.Do(http.MethodPut, location.RequestURI(), nil, nil)
}

// CloseUploadWithContent performs a PUT to the upload location to close the upload session
// while uploading a final chunk. It is functionally the same as a the PUT request
// in a monolithic POST then PUT upload.
func (c *client) CloseUploadWithContent(location *url.URL, digest digest.Digest, content []byte, written int) *http.Response {
	buf := bytes.NewBuffer(content)
	size := len(content)
	rnge := fmt.Sprintf("%d-%d", written, written+size-1)

	headers := make(http.Header)
	headers.Set("Content-Type", "application/octet-stream")
	headers.Set("Content-Range", rnge)
	headers.Set("Content-Length", strconv.Itoa(len(content)))

	query := location.Query()
	query.Add("digest", digest.String())
	location.RawQuery = query.Encode()

	return c.Do(http.MethodPut, location.RequestURI(), headers, buf)
}

func (c *client) UploadBlobChunk(location *url.URL, chunk []byte, written int) *http.Response {
	buf := bytes.NewBuffer(chunk)
	size := len(chunk)
	rnge := fmt.Sprintf("%d-%d", written, written+size-1)

	headers := make(http.Header)
	headers.Set("Content-Type", "application/octet-stream")
	headers.Set("Content-Range", rnge)
	headers.Set("Content-Length", strconv.Itoa(size))

	return c.Do(http.MethodPatch, location.RequestURI(), headers, buf)
}

func (c *client) UploadBlobStream(location *url.URL, content io.Reader) *http.Response {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/octet-stream")

	return c.Do(http.MethodPatch, location.RequestURI(), headers, content)
}

func (c *client) Do(method string, path string, headers http.Header, body io.Reader) *http.Response {
	c.t.Helper()

	req := httptest.NewRequest(method, path, body)
	req.Header = headers
	resp := httptest.NewRecorder()

	c.handler.ServeHTTP(resp, req)

	return resp.Result()
}

func Pointer[K any](val K) *K {
	return &val
}
