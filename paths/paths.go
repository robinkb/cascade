package paths

import (
	"fmt"

	"github.com/opencontainers/go-digest"
)

/*
BlobStore
├── blobs
│   └── <algorithm>
│       └── <hash>[0:2]
│           └── <hash>
│               └── data
└── uploads
    └── <session-id>
        └── data

MetaStore
└── <repository>
    ├── layers
    │   └── <algorithm>
    │       └── <hash>
    │           └── link
    ├── manifests
    │   └── <algorithm>
    │       └── <hash>
    │           └── link
    ├── tags
    │   └── <tag>
    │       └── link
    └── uploads
        └── <sessionId>
            ├── hashState
            │   └── <algorithm>
            │       └── state
            ├── link
            └── startTime
*/

var BlobStore blobStore

type blobStore struct{}

func (b *blobStore) BlobData(digest digest.Digest) string {
	return fmt.Sprintf("blobs/%s/%s/%s/data", digest.Algorithm(), digest.Encoded()[0:2], digest.Encoded())
}

func (b *blobStore) UploadData(sessionID string) string {
	return fmt.Sprintf("uploads/%s/data", sessionID)
}

var MetaStore metaStore

type metaStore struct{}

func (m *metaStore) BlobLink(repository string, digest digest.Digest) string {
	return fmt.Sprintf("%s/blobs/%s/%s/link", repository, digest.Algorithm(), digest.Encoded())
}

func (m *metaStore) ManifestLink(repository string, digest digest.Digest) string {
	return fmt.Sprintf("%s/manifests/%s/%s/link", repository, digest.Algorithm(), digest.Encoded())
}

func (m *metaStore) TagLink(repository, tag string) string {
	return fmt.Sprintf("%s/tags/%s/link", repository, tag)
}

func (m *metaStore) UploadHashState(repository, sessionID string, algorithm digest.Algorithm) string {
	return fmt.Sprintf("%s/uploads/%s/hashState/%s/state", repository, sessionID, algorithm)
}

func (m *metaStore) UploadLink(repository, sessionID string) string {
	return fmt.Sprintf("%s/uploads/%s/link", repository, sessionID)
}

func (m *metaStore) UploadStartTime(repository, sessionID string) string {
	return fmt.Sprintf("%s/uploads/%s/startTime", repository, sessionID)
}
