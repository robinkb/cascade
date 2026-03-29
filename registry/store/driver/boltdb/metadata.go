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
	// manifestBuckets list the default buckets in a manifest.
	manifestBuckets = [][]byte{
		_MANIFESTS, _TAGS,
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
		if !blob.found() {
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
	r.blobs(tx).addBlob(id).addOwner(r.name)
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
		if !blob.found() {
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
	var meta store.Manifest

	err := r.db.View(func(tx *bolt.Tx) error {
		manifest := r.repository(tx).manifests().manifest(id)
		if !manifest.found() {
			return fmt.Errorf("%w: %s", store.ErrManifestNotFound, id)
		}
		meta = manifest.metadata()
		return nil
	})
	if err != nil {
		return meta, err
	}

	return meta, nil
}

func (r *repositoryStore) PutManifest(id digest.Digest, meta store.Manifest, refs store.References) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		repo := r.repository(tx)

		blobs := repo.blobs()
		if refs.Config != "" {
			configBlob := blobs.blob(refs.Config)
			if !configBlob.found() {
				return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestConfigNotFound, refs.Config)
			}
			configBlob.addOwner(id)
		}

		for _, layerDigest := range refs.Layers {
			layerBlob := blobs.blob(layerDigest)
			if !layerBlob.found() {
				return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestLayerNotFound, layerDigest)
			}
			layerBlob.addOwner(id)
		}

		manifests := repo.manifests()
		for _, manifestDigest := range refs.Manifests {
			manifest := manifests.manifest(manifestDigest)
			if !manifest.found() {
				return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestImageNotFound, manifestDigest)
			}
			manifest.addManifestOwner(id)
		}

		manifests.addManifest(id, meta, refs)
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

	repo := r.repository(tx)
	manifests := repo.manifests()
	manifest := manifests.manifest(id)
	if !manifest.found() {
		return nil, fmt.Errorf("%w: %s", store.ErrManifestNotFound, id)
	}

	if manifest.hasOwners() {
		return nil, fmt.Errorf("%w: %s", store.ErrManifestInUse, id)
	}

	refs := manifest.references()

	blobs := repo.blobs()
	if refs.Config != "" {
		blobs.blob(refs.Config).removeOwner(id)
		if err := r.deleteBlob(tx, refs.Config); err != nil {
			if !errors.Is(err, store.ErrBlobInUse) {
				return nil, err
			}
		} else {
			deleted = append(deleted, refs.Config)
		}
	}

	for _, layerDigest := range refs.Layers {
		blobs.blob(layerDigest).removeOwner(id)
		if err := r.deleteBlob(tx, layerDigest); err != nil {
			if errors.Is(err, store.ErrBlobInUse) {
				continue
			}
			return nil, err
		}
		deleted = append(deleted, layerDigest)
	}

	for _, manifestDigest := range refs.Manifests {
		manifests.manifest(manifestDigest).removeManifestOwner(id)
		digests, err := r.deleteManifest(tx, manifestDigest)
		if err != nil {
			if errors.Is(err, store.ErrManifestInUse) {
				continue
			}
			return nil, err
		}
		deleted = append(deleted, digests...)
	}

	manifests.removeManifest(id)

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
	return sharedBlob{o.b.Bucket([]byte(id))}
}

func (o sharedBlobs) addBlob(id digest.Digest) sharedBlob {
	b, _ := o.b.CreateBucketIfNotExists([]byte(id))
	return sharedBlob{b}
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
	return repoBlobs{o.b.Bucket(_BLOBS)}
}

func (o repository) manifests() manifests {
	return manifests{o.b.Bucket(_MANIFESTS)}
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

func (o repoBlob) found() bool { return o.b != nil }

func (o repoBlob) addOwner(id digest.Digest) {
	o.b.Put([]byte(id), nil)
}

func (o repoBlob) removeOwner(id digest.Digest) {
	o.b.Delete([]byte(id))
}

func (o repoBlob) hasOwners() bool {
	return o.b.Inspect().KeyN != 0
}

type manifests struct {
	b *bbolt.Bucket
}

func (o manifests) manifest(id digest.Digest) manifest {
	return manifest{o.b.Bucket([]byte(id))}
}

func (o manifests) addManifest(id digest.Digest, meta store.Manifest, refs store.References) manifest {
	b, _ := o.b.CreateBucket([]byte(id))
	for _, bucket := range manifestBuckets {
		b.CreateBucket(bucket)
	}

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(&meta); err != nil {
		panic(err)
	}
	b.Put(_METADATA, buf.Bytes())

	buf = new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(&refs); err != nil {
		panic(err)
	}
	b.Put(_REFERENCES, buf.Bytes())

	return manifest{b}
}

func (o manifests) removeManifest(id digest.Digest) {
	o.b.DeleteBucket([]byte(id))
}

type manifest struct {
	b *bbolt.Bucket
}

func (o manifest) found() bool { return o.b != nil }

func (o manifest) metadata() (meta store.Manifest) {
	data := o.b.Get(_METADATA)
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&meta); err != nil {
		panic(err)
	}
	return
}

func (o manifest) references() (refs store.References) {
	data := o.b.Get(_REFERENCES)
	buf := bytes.NewBuffer(data)
	gob.NewDecoder(buf).Decode(&refs)
	return
}

func (o manifest) addManifestOwner(id digest.Digest) {
	o.b.Bucket(_MANIFESTS).Put([]byte(id), nil)
}

func (o manifest) removeManifestOwner(id digest.Digest) {
	o.b.Bucket(_MANIFESTS).Delete([]byte(id))
}

func (o manifest) hasOwners() bool {
	return o.b.Bucket(_MANIFESTS).Inspect().KeyN != 0 ||
		o.b.Bucket(_TAGS).Inspect().KeyN != 0
}
