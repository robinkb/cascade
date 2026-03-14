package store

import (
	"errors"
	"io"
)

// TODO: Allow passing more than one source so that multiple sources can be checked.
func Reconcile(meta Metadata, blobs Blobs, src BlobReader) error {
	digests, err := meta.ListBlobs()
	if err != nil {
		return err
	}

	for _, d := range digests {
		if _, err := blobs.StatBlob(d); err != nil {
			if errors.Is(err, ErrNotFound) {
				r, err := src.BlobReader(d)
				if err != nil {
					return err
				}

				w, err := blobs.BlobWriter(d)
				if err != nil {
					return err
				}

				_, err = io.Copy(w, r)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	return nil
}
