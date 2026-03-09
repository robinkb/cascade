package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/golang/protobuf/proto"
	godigest "github.com/opencontainers/go-digest"
	"go.etcd.io/raft/v3/raftpb"
)

func NewClient(baseUrl string) *Client {
	return &Client{
		client:  new(http.Client),
		baseUrl: baseUrl,
	}
}

type Client struct {
	client  *http.Client
	baseUrl string
}

func (c *Client) SendMessage(m *raftpb.Message) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}

	resp, err := c.do(http.MethodPost, "/message", nil, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("unexpected error")
	}
	return nil
}

// TODO: Plainly does not belong here.
func (c *Client) BlobReader(id godigest.Digest) (io.Reader, error) {
	path := fmt.Sprintf("/blobs/%s", id.String())
	log.Print("reading blob at ", path)
	resp, err := c.do(http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *Client) do(method string, path string, headers http.Header, body io.Reader) (*http.Response, error) {
	url := c.baseUrl + path
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header = headers

	return c.client.Do(req)
}
