package boltdb

import (
	"bytes"
	"encoding/gob"
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
	_REFERENCES   = []byte("REFERENCES")
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
		for id, _ := c.First(); id != nil; id, _ = c.Next() {
			owners := sharedBlobs.Bucket(id)
			owners.Delete([]byte(name))
			if keys(owners) == 0 {
				sharedBlobs.DeleteBucket(id)
			}
		}

		return tx.Bucket(_REPOSITORIES).DeleteBucket([]byte(name))
	})
}

func (m *metadataStore) Blobs() iter.Seq[digest.Digest] {
	return func(yield func(digest.Digest) bool) {
		err := m.db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket(_BLOBS).Cursor()
			for id, _ := c.First(); id != nil; id, _ = c.Next() {
				if !yield(digest.Digest(id)) {
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
		blob := r.repository(tx).blobs().blob(id)
		if blob.b == nil {
			return fmt.Errorf("%w: %s", store.ErrBlobNotFound, id)
		}
		return nil
	})
}

func (r *repositoryStore) PutBlob(id digest.Digest) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		return r.putBlob(tx, id)
	})
}

func (r *repositoryStore) putBlob(tx *bolt.Tx, id digest.Digest) error {
	r.blobs(tx).blob(id).addOwner(r.name)
	r.repository(tx).blobs().addBlob(id)
	return nil
}

func (r *repositoryStore) DeleteBlob(id digest.Digest) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		return r.deleteBlob(tx, id)
	})
}

func (r *repositoryStore) deleteBlob(tx *bolt.Tx, id digest.Digest) error {
	{ // repository blobs
		blobs := r.repository(tx).blobs()
		blob := blobs.blob(id)
		if blob.b == nil {
			return fmt.Errorf("%w: %s", store.ErrBlobNotFound, id)
		}
		if blob.hasOwners() {
			return fmt.Errorf("%w: %s", store.ErrBlobInUse, id)
		}

		blobs.removeBlob(id)
	}
	{ // shared blobs
		blobs := r.blobs(tx)
		blob := blobs.blob(id)
		blob.removeOwner(r.name)
		if !blob.hasOwners() {
			blobs.removeBlob(id)
		}
	}

	return nil
}

func (r *repositoryStore) GetManifest(id digest.Digest) (store.Manifest, error) {
	var buf *bytes.Buffer
	var meta store.Manifest

	err := r.db.View(func(tx *bolt.Tx) error {
		manifest := r.repo(tx).Bucket(_MANIFESTS).Bucket([]byte(id))
		if manifest == nil {
			return fmt.Errorf("%w: %s", store.ErrManifestNotFound, id)
		}
		data := manifest.Get(_METADATA)
		buf = bytes.NewBuffer(data)
		return nil
	})
	if err != nil {
		return store.Manifest{}, err
	}

	if err := gob.NewDecoder(buf).Decode(&meta); err != nil {
		return store.Manifest{}, err
	}

	return meta, nil
}

func (r *repositoryStore) PutManifest(id digest.Digest, meta store.Manifest, refs store.References) error {
	bufMeta, bufRefs := new(bytes.Buffer), new(bytes.Buffer)
	err := gob.NewEncoder(bufMeta).Encode(&meta)
	if err != nil {
		return err
	}
	err = gob.NewEncoder(bufRefs).Encode(&refs)
	if err != nil {
		return err
	}

	return r.db.Update(func(tx *bolt.Tx) error {
		manifest, err := r.repo(tx).Bucket(_MANIFESTS).CreateBucket([]byte(id))
		if err != nil {
			return err
		}

		_, err = manifest.CreateBucket(_MANIFESTS)
		if err != nil {
			return err
		}

		err = manifest.Put(_METADATA, bufMeta.Bytes())
		if err != nil {
			return err
		}

		err = manifest.Put(_REFERENCES, bufRefs.Bytes())
		if err != nil {
			return err
		}

		if refs.Config != "" {
			owners := r.repo(tx).Bucket(_BLOBS).Bucket([]byte(refs.Config))
			if owners == nil {
				return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestConfigNotFound, refs.Config)
			}
			owners.Put([]byte(id), []byte{})
		}

		for _, layerDigest := range refs.Layers {
			owners := r.repo(tx).Bucket(_BLOBS).Bucket([]byte(layerDigest))
			if owners == nil {
				return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestLayerNotFound, layerDigest)
			}
			owners.Put([]byte(id), []byte{})
		}

		for _, manifestDigest := range refs.Manifests {
			manifest := r.repo(tx).Bucket(_MANIFESTS).Bucket([]byte(manifestDigest))
			if manifest == nil {
				return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestImageNotFound, manifestDigest)
			}
			manifest.Bucket(_MANIFESTS).Put([]byte(manifestDigest), nil)
		}

		return r.putBlob(tx, id)
	})
}

func (r *repositoryStore) DeleteManifest(id digest.Digest) ([]digest.Digest, error) {
	deleted := make([]digest.Digest, 0)
	err := r.db.Update(func(tx *bolt.Tx) error {
		digests, err := r.deleteManifest(tx, id)
		if err != nil {
			return err
		}
		deleted = append(deleted, digests...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return deleted, nil
}

func (r *repositoryStore) deleteManifest(tx *bbolt.Tx, id digest.Digest) ([]digest.Digest, error) {
	deleted := make([]digest.Digest, 0)

	manifest := r.repo(tx).Bucket(_MANIFESTS).Bucket([]byte(id))
	if manifest == nil {
		return nil, fmt.Errorf("%w: %s", store.ErrManifestNotFound, id)
	}

	if keys(manifest.Bucket(_MANIFESTS)) != 0 {
		return nil, fmt.Errorf("%w: %s", store.ErrManifestInUse, id)
	}

	var refs store.References
	refsData := manifest.Get(_REFERENCES)
	err := gob.NewDecoder(bytes.NewBuffer(refsData)).Decode(&refs)
	if err != nil {
		return nil, err
	}

	if refs.Config != "" {
		r.repo(tx).Bucket(_BLOBS).Bucket([]byte(refs.Config)).Delete([]byte(id))
		if err := r.deleteBlob(tx, refs.Config); err != nil {
			if !errors.Is(err, store.ErrBlobInUse) {
				return nil, err
			}
		} else {
			deleted = append(deleted, refs.Config)
		}
	}

	for _, layerDigest := range refs.Layers {
		r.repo(tx).Bucket(_BLOBS).Bucket([]byte(layerDigest)).Delete([]byte(id))
		if err := r.deleteBlob(tx, layerDigest); err != nil {
			if errors.Is(err, store.ErrBlobInUse) {
				continue
			}
			return nil, err
		}
		deleted = append(deleted, layerDigest)
	}

	for _, manifestDigest := range refs.Manifests {
		r.repo(tx).Bucket(_MANIFESTS).Bucket([]byte(manifestDigest)).Delete([]byte(id))
		digests, err := r.deleteManifest(tx, manifestDigest)
		if err != nil {
			if errors.Is(err, store.ErrManifestInUse) {
				continue
			}
			return nil, err
		}
		deleted = append(deleted, digests...)
	}

	if err := r.repo(tx).Bucket(_MANIFESTS).DeleteBucket([]byte(id)); err != nil {
		return nil, err
	}

	if err := r.deleteBlob(tx, id); err != nil {
		return nil, err
	}

	deleted = append(deleted, id)
	return deleted, nil
}

func (r *repositoryStore) blobs(tx *bbolt.Tx) sharedBlobs {
	return sharedBlobs{
		b: tx.Bucket(_BLOBS),
	}
}

// repo returns the bucket for this repository.
func (r *repositoryStore) repo(tx *bbolt.Tx) *bbolt.Bucket {
	return tx.Bucket(_REPOSITORIES).Bucket([]byte(r.name))
}

func (r *repositoryStore) repository(tx *bbolt.Tx) repository {
	return repository{
		b: tx.Bucket(_REPOSITORIES).Bucket([]byte(r.name)),
	}
}

func keys(b *bbolt.Bucket) (n int) {
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		n++
	}
	return
}

// sharedBlobs contains the metadata of all the blobs in the blob store.
type sharedBlobs struct {
	b *bbolt.Bucket
}

func (o sharedBlobs) blob(id digest.Digest) sharedBlob {
	b, _ := o.b.CreateBucketIfNotExists([]byte(id))
	return sharedBlob{
		b: b,
	}
}

func (o sharedBlobs) removeBlob(id digest.Digest) {
	o.b.DeleteBucket([]byte(id))
}

// sharedBlob represents a single blob in the blob store.
// It tracks which repositories owns each blob.
type sharedBlob struct {
	b *bbolt.Bucket
}

func (o sharedBlob) addOwner(name string) {
	o.b.Put([]byte(name), nil)
}

func (o sharedBlob) removeOwner(name string) {
	o.b.Delete([]byte(name))
}

func (o sharedBlob) hasOwners() bool {
	return o.b.Inspect().KeyN != 0
}

// repository contains the metadata of a single repository.
type repository struct {
	b *bbolt.Bucket
}

func (o repository) blobs() repoBlobs {
	return repoBlobs{
		b: o.b.Bucket(_BLOBS),
	}
}

// repoBlobs contains metadata of blobs belonging to a specific repository.
type repoBlobs struct {
	b *bbolt.Bucket
}

func (o repoBlobs) blob(id digest.Digest) repoBlob {
	return repoBlob{o.b.Bucket([]byte(id))}
}

func (o repoBlobs) addBlob(id digest.Digest) {
	o.b.CreateBucket([]byte(id))
}

func (o repoBlobs) removeBlob(id digest.Digest) {
	o.b.DeleteBucket([]byte(id))
}

type repoBlob struct {
	b *bbolt.Bucket
}

func (o repoBlob) hasOwners() bool {
	return o.b.Inspect().KeyN != 0
}
