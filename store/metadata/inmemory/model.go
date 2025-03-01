package inmemory

import "github.com/opencontainers/go-digest"

type (
	MetadataStore struct {
		repositories map[string]*Repository
		blobs        map[string]string
	}

	Repository struct {
		blobs     map[string]*Blob
		manifests map[string]*Manifest
		tags      map[string]*Tag
	}

	Manifest struct {
		path string
	}

	Blob struct {
		path string
	}

	Tag struct {
		digest digest.Digest
	}
)
