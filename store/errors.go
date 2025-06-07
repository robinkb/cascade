package store

import "errors"

var (
	ErrRepositoryNotFound = errors.New("repository not found in metadata store")
	ErrBlobNotFound       = errors.New("blob not found in blob store")
	ErrMetadataNotFound   = errors.New("metadata not found in metadata store")
	ErrNotFound           = errors.New("not found")
)
