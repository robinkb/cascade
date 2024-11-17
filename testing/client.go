package testing

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/opencontainers/go-digest"
)

// NewClient returns an initialized Client for the given base URL.
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
	baseUrl string
	t       *testing.T
}

func (c *Client) CheckBlob(name string, digest digest.Digest) *http.Response {
	return c.unwrap(
		http.Head(fmt.Sprintf("%s/v2/%s/blobs/%s", c.baseUrl, name, digest)),
	)
}

func (c *Client) GetBlob(name string, digest digest.Digest) *http.Response {
	return c.unwrap(
		http.Get(fmt.Sprintf("%s/v2/%s/blobs/%s", c.baseUrl, name, digest)),
	)
}

func (c *Client) CheckManifest(name string, digest digest.Digest) *http.Response {
	return c.unwrap(
		http.Head(fmt.Sprintf("%s/v2/%s/manifests/%s", c.baseUrl, name, digest)),
	)
}

func (c *Client) GetManifest(name string, digest digest.Digest) *http.Response {
	return c.unwrap(
		http.Get(fmt.Sprintf("%s/v2/%s/manifests/%s", c.baseUrl, name, digest)),
	)
}

func (c *Client) unwrap(resp *http.Response, err error) *http.Response {
	if err != nil {
		c.t.Fatalf("unexpected HTTP error: %s", err)
	}
	return resp
}
