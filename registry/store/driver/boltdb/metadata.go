package boltdb

import (
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

var (
	_BLOBS        = []byte("BLOBS")
	_MANIFESTS    = []byte("MANIFESTS")
	_METADATA     = []byte("METADATA")
	_REFERRERS    = []byte("REFERRERS")
	_REPOSITORIES = []byte("REPOSITORIES")
	_TAGS         = []byte("TAGS")
	_UPLOADS      = []byte("UPLOADS")

	// repositoryBuckets lists the default buckets in a repository.
	repositoryBuckets = [][]byte{
		_BLOBS, _MANIFESTS, _TAGS, _UPLOADS,
	}
)

func NewMetadataStore(path string) (store.Metadata, error) {
	path = filepath.Join(path, "metadata.db")

	opts := &bolt.Options{
		Timeout: 1 * time.Second,
	}

	db, err := bolt.Open(path, 0600, opts)
	if err != nil {
		return nil, err
	}

	// Initialize top-level buckets that hold all repositories,
	// and cross-repository blob references.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(_BLOBS)
		if err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(_REPOSITORIES)
		return err
	}); err != nil {
		return nil, err
	}

	return &metadataStore{
		db: db,
	}, nil
}

type metadataStore struct {
	store.Metadata

	db *bolt.DB
}

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	err := m.db.Update(func(tx *bolt.Tx) error {
		repo, err := tx.Bucket(_REPOSITORIES).CreateBucket([]byte(name))
		if err != nil {
			if errors.Is(err, bolterrors.ErrBucketExists) {
				return fmt.Errorf("%w: %s", store.ErrRepositoryExists, name)
			}
		}

		for _, bucket := range repositoryBuckets {
			if _, err := repo.CreateBucket(bucket); err != nil {
				return err
			}
		}
		return nil
	})

	return newRepositoryStore(name, m.db), err
}

func (m *metadataStore) GetRepository(name string) (store.Repository, error) {
	err := m.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrRepositoryNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return newRepositoryStore(name, m.db), nil
}

func (m *metadataStore) DeleteRepository(name string) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrRepositoryNotFound
		}

		c := repo.Bucket(_BLOBS).Cursor()
		sharedBlobs := tx.Bucket(_BLOBS)
		for digest, _ := c.First(); digest != nil; digest, _ = c.Next() {
			sharedBlobs.Bucket(digest).Delete([]byte(name))
			if keys(sharedBlobs.Bucket(digest)) == 0 {
				sharedBlobs.DeleteBucket(digest)
			}
		}

		return tx.Bucket(_REPOSITORIES).DeleteBucket([]byte(name))
	})
}

func (m *metadataStore) Blobs() iter.Seq[digest.Digest] {
	return func(yield func(digest.Digest) bool) {
		err := m.db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket(_BLOBS).Cursor()

			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if !yield(digest.Digest(k)) {
					return nil
				}
			}

			return nil
		})
		if err != nil {
			panic(err)
		}
	}
}

func newRepositoryStore(name string, db *bbolt.DB) store.Repository {
	return &repositoryStore{
		name: name,
		db:   db,
	}
}

type repositoryStore struct {
	store.Repository
	name string
	db   *bbolt.DB
}

func (r *repositoryStore) GetBlob(id digest.Digest) error {
	return r.db.View(func(tx *bolt.Tx) error {
		if r.self(tx).Bucket(_BLOBS).Bucket([]byte(id)) == nil {
			return store.ErrBlobNotFound
		}
		return nil
	})
}

func (r *repositoryStore) PutBlob(id digest.Digest) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		sharedBlob, err := tx.Bucket(_BLOBS).CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return err
		}
		err = sharedBlob.Put([]byte(r.name), nil)
		if err != nil {
			return err
		}

		_, err = r.self(tx).Bucket(_BLOBS).CreateBucket([]byte(id))
		return err
	})
}

func (r *repositoryStore) DeleteBlob(id digest.Digest) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		sharedBlob := tx.Bucket(_BLOBS).Bucket([]byte(id))
		if sharedBlob == nil {
			return store.ErrBlobNotFound
		}
		sharedBlob.Delete([]byte(r.name))
		if keys(sharedBlob) == 0 {
			err := tx.Bucket(_BLOBS).DeleteBucket([]byte(id))
			if err != nil {
				return err
			}
		}

		blobs := r.self(tx).Bucket(_BLOBS)
		if blobs.Bucket([]byte(id)) == nil {
			return store.ErrBlobNotFound
		}

		return blobs.DeleteBucket([]byte(id))
	})
}

// self returns the root bucket for this repository.
func (r *repositoryStore) self(tx *bbolt.Tx) *bbolt.Bucket {
	return tx.Bucket(_REPOSITORIES).Bucket([]byte(r.name))
}

func keys(b *bbolt.Bucket) (n int) {
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		n++
	}
	return
}
