package testing

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/opencontainers/go-digest"
)

// NewClient returns an initialized Client for the given base URL.
// A client should be used in the (sub)test where it is created.
// The given url should be of the form 'http://ipaddr:port' as returned by httptest.Server.URL.
func NewClient(t *testing.T, url string) *Client {
	return &Client{
		baseUrl: url,
		t:       t,
	}
}

// Client is a simple registry client meant for use in testing.
// It will automatically fail tests if it encounters an unexpected error.
// Users should initialize it with NewClient().
type Client struct {
	http    http.Client
	baseUrl string
	t       *testing.T
}

func (c *Client) CheckBlob(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
	return c.Do(http.MethodHead, path, nil, nil)
}

func (c *Client) GetBlob(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
	return c.Do(http.MethodGet, path, nil, nil)
}

func (c *Client) CheckManifest(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, digest)
	return c.Do(http.MethodHead, path, nil, nil)
}

func (c *Client) GetManifest(name string, digest digest.Digest) *http.Response {
	path := fmt.Sprintf("/v2/%s/manifests/%s", name, digest)
	return c.Do(http.MethodGet, path, nil, nil)
}

func (c *Client) InitUpload(name string) *http.Response {
	path := fmt.Sprintf("/v2/%s/blobs/uploads/", name)
	return c.Do(http.MethodPost, path, nil, nil)
}

func (c *Client) UploadBlob(location string, digest digest.Digest, content []byte) *http.Response {
	url, err := url.ParseRequestURI(location)
	if err != nil {
		c.t.Fatalf("failed to parse location: %s", err)
	}

	query := url.Query()
	query.Add("digest", digest.String())
	url.RawQuery = query.Encode()

	headers := make(http.Header)
	headers.Set("Content-Length", strconv.Itoa(len(content)))
	headers.Set("Content-Type", "application/octet-stream")

	return c.Do(http.MethodPut, url.RequestURI(), headers, bytes.NewBuffer(content))
}

func (c *Client) Do(method string, path string, header http.Header, body io.Reader) *http.Response {
	c.t.Helper()

	url := c.baseUrl + path
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		c.t.Fatalf("failed to build request: %s", err)
	}

	req.Header = header

	resp, err := c.http.Do(req)
	if err != nil {
		c.t.Fatalf("unexpected HTTP error: %s", err)
	}
	return resp
}

func (c *Client) unwrap(resp *http.Response, err error) *http.Response {
	if err != nil {
		c.t.Fatalf("unexpected HTTP error: %s", err)
	}
	return resp
}
