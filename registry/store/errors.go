package store

import "errors"

var (

	// Blob errors
	ErrBlobNotFound = errors.New("blob not found in blob store")

	ErrMetadataNotFound = errors.New("metadata not found in metadata store")
	// TODO: This should be replaced by the less generic ones.
	ErrNotFound = errors.New("not found")
)
