package store

import (
	"fmt"
	"io"
	"iter"
	"net/http"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

type (
	// Blobs defines the interface for storing the actual data of the registry.
	// Implementations of this interface are responsible for deciding how data is persisted.
	// Blobs must be retrievable by their digest, and uploads by their session ID.
	Blobs interface {
		// AllBlobs iterates over all blobs in the store.
		// TODO: Get rid of this. iter.Seq2 is an awkward fit for this.
		// See: https://github.com/golang/go/issues/71901#issuecomment-3366650845
		// Another example is BoltDB's Cursor.
		AllBlobs() iter.Seq2[digest.Digest, error]
		// StatBlob returns basic file info about the blob with the given digest.
		StatBlob(id digest.Digest) (*BlobInfo, error)
		// GetBlob returns the blob at the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		GetBlob(id digest.Digest) ([]byte, error)
		BlobReader
		// BlobWriter returns an io.Writer to write a blob in a streaming fashion.
		// It should only be used for reconciliation. Clients must go through the upload flow.
		// TODO: Get rid of this. Reconciliation should also go through the upload flow for the same reason as clients.
		BlobWriter(id digest.Digest) (io.Writer, error)
		// PutBlob writes content to the given path. Intended for smaller blobs that
		// must be fully read into memory server-side, like manifests.
		// Put does not append and always writes the entire blob.
		PutBlob(id digest.Digest, content []byte) error
		// DeleteBlob removes a blob from the blob store.
		DeleteBlob(id digest.Digest) error

		// StatBlob returns basic file info about the upload with the given UUID.
		StatUpload(id uuid.UUID) (*BlobInfo, error)
		// InitUpload prepares the blob store to start an upload. In most implementations,
		// it will create an empty file on the blob store that will later be appended.
		InitUpload(id uuid.UUID) error
		// UploadWriter returns an io.Writer to write to an initialized upload.
		// Uploads are always uploaded in order and appended to. If an upload fails or must be truncated,
		// a new session must be started instead.
		UploadWriter(id uuid.UUID) (io.WriteCloser, error)
		// CloseUpload finishes an upload and makes its contents accessible in the blob store by its digest.
		// In some implementations, this may effectively be a rename.
		CloseUpload(id uuid.UUID, digest digest.Digest) error
		// DeleteUpload removes an upload from the store.
		// Intended for cleaning up expired or failed uploads.
		DeleteUpload(id uuid.UUID) error
	}

	BlobReader interface {
		// BlobReader returns an io.Reader that can be used to read a blob in a streaming fashion.
		BlobReader(id digest.Digest) (io.Reader, error)
	}

	// BlobInfo contains the basic information of a blob.
	BlobInfo struct {
		Size int64 // TODO: Store and retrieve in the metadata store instead. Never changes and is faster.
	}
)

func NewBlobsClient(baseUrl string) *blobsClient {
	return &blobsClient{
		client:  new(http.Client),
		baseUrl: baseUrl,
	}
}

type blobsClient struct {
	client  *http.Client
	baseUrl string
}

func (c *blobsClient) BlobReader(digest digest.Digest) (io.Reader, error) {
	path := fmt.Sprintf("/blobs/%s", digest.String())
	resp, err := c.do(http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *blobsClient) do(method string, path string, headers http.Header, body io.Reader) (*http.Response, error) {
	url := c.baseUrl + path
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header = headers

	return c.client.Do(req)
}
