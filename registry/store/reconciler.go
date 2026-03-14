package store

import (
	"errors"
	"fmt"
	"io"

	"github.com/robinkb/cascade/cluster"
)

func NewReconciler(meta Metadata, blobs Blobs) *reconciler {
	return &reconciler{
		meta:  meta,
		blobs: blobs,
	}
}

type reconciler struct {
	meta  Metadata
	blobs Blobs
}

func (r *reconciler) Snapshot(w io.Writer) error {
	return r.meta.Snapshot(w)
}

func (r *reconciler) Restore(rd io.Reader, peers []cluster.Peer) error {
	if err := r.meta.Restore(rd); err != nil {
		return err
	}

	clients := cluster.NewClients[blobsClient]()
	for _, peer := range peers {
		client := NewBlobsClient(fmt.Sprintf("http://%s/store", peer.AddrPort.String()))
		if err := clients.Add(peer, client); err != nil {
			return err
		}
	}

	digests, err := r.meta.ListBlobs()
	if err != nil {
		return err
	}

	for _, digest := range digests {
		_, err := r.blobs.StatBlob(digest)
		if err == nil {
			continue
		}

		if !errors.Is(err, ErrNotFound) {
			return err
		}

		for _, peer := range peers {
			src, err := clients.Get(peer.ID)
			if err != nil {
				return err
			}

			rd, err := src.BlobReader(digest)
			if err != nil {
				continue
			}

			wr, err := r.blobs.BlobWriter(digest)
			if err != nil {
				return err
			}

			_, err = io.Copy(wr, rd)
			if err != nil {
				return err
			}

			break
		}
	}

	return nil
}
