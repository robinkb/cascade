package store

import (
	"errors"
	"fmt"
	"io"

	"github.com/robinkb/cascade/cluster"
)

func NewRestorer(meta Metadata, blobs Blobs) *restorer {
	return &restorer{
		meta:  meta,
		blobs: blobs,
	}
}

type restorer struct {
	meta  Metadata
	blobs Blobs
}

func (r *restorer) Snapshot(w io.Writer) error {
	return r.meta.Snapshot(w)
}

func (r *restorer) Restore(rd io.Reader, peer cluster.Peer) error {
	if err := r.meta.Restore(rd); err != nil {
		return err
	}

	client := NewBlobsClient(fmt.Sprintf("http://%s/store", peer.AddrPort.String()))
	return Reconcile(r.meta, r.blobs, client)
}

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
