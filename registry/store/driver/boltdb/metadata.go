package boltdb

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
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
		_REFERRERS, _MANIFESTS, _TAGS,
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
		db:   db,
		opts: opts,
	}, nil
}

type metadataStore struct {
	store.Metadata

	db   *bolt.DB
	opts *bolt.Options
}

func (m *metadataStore) ListRepositories(count int, last string) ([]string, error) {
	result := make([]string, 0)

	err := m.db.View(func(tx *bolt.Tx) error {
		repos := tx.Bucket(_REPOSITORIES)
		c := repos.Cursor()

		k, _ := c.Seek([]byte(last))
		if last != "" {
			if repo := repos.Bucket([]byte(last)); repo == nil {
				return fmt.Errorf("%w: %s", store.ErrRepositoryNotFound, last)
			}
			// If 'last' is not empty, we should start at the name after the last one.
			k, _ = c.Next()
		}
		for i := 0; k != nil && (count == -1 || i < count); i++ {
			result = append(result, string(k))
			k, _ = c.Next()
		}

		return nil
	})

	return result, err
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
			_ = owners.Delete([]byte(name))
			if owners.Inspect().KeyN == 0 {
				_ = sharedBlobs.DeleteBucket(id)
			}
		}

		return tx.Bucket(_REPOSITORIES).DeleteBucket([]byte(name))
	})
}

func (m *metadataStore) Blobs() iter.Seq[digest.Digest] {
	return func(yield func(digest.Digest) bool) {
		_ = m.db.View(func(tx *bolt.Tx) error {
			return tx.Bucket(_BLOBS).ForEachBucket(func(id []byte) error {
				if !yield(digest.Digest(id)) {
					return nil
				}
				return nil
			})
		})
	}
}

func (m *metadataStore) Snapshot(w io.Writer) error {
	return m.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

func (m *metadataStore) Restore(r io.Reader) error {
	path := m.db.Path()
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}

	err = m.db.Close()
	if err != nil {
		return err
	}
	err = os.Rename(tmp, path)
	if err != nil {
		return err
	}
	db, err := bolt.Open(path, 0600, m.opts)
	if err != nil {
		return err
	}

	m.db = db
	return nil
}

func newRepositoryStore(name string, db *bolt.DB) store.Repository {
	return &repositoryStore{
		name: name,
		db:   db,
	}
}

type repositoryStore struct {
	store.Repository
	name string
	db   *bolt.DB
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

		if refs.Subject != "" {
			manifest := manifests.manifest(refs.Subject)
			if !manifest.found() {
				return fmt.Errorf("%w: %w: %s", store.ErrManifestInvalid, store.ErrManifestSubjectNotFound, refs.Subject)
			}
			manifest.addReferrer(id)
		}

		manifests.addManifest(id, meta, refs)
		if err := r.putBlob(tx, id); err != nil {
			return err
		}
		blobs.blob(id).addOwner(id)
		return nil
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

func (r *repositoryStore) deleteManifest(tx *bolt.Tx, id digest.Digest) ([]digest.Digest, error) {
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
		blob := blobs.blob(layerDigest)
		if !blob.found() {
			continue
		}
		blob.removeOwner(id)

		if err := r.deleteBlob(tx, layerDigest); err != nil {
			if errors.Is(err, store.ErrBlobInUse) {
				continue
			}
			return nil, err
		}
		deleted = append(deleted, layerDigest)
	}

	for _, manifestDigest := range refs.Manifests {
		manifest := manifests.manifest(manifestDigest)
		if !manifest.found() {
			continue
		}
		manifest.removeManifestOwner(id)

		digests, err := r.deleteManifest(tx, manifestDigest)
		if err != nil {
			if errors.Is(err, store.ErrManifestInUse) {
				continue
			}
			return nil, err
		}
		deleted = append(deleted, digests...)
	}

	if refs.Subject != "" {
		manifests.manifest(refs.Subject).removeReferrer(id)
	}

	for referrerDigest := range manifest.referrers() {
		digests, err := r.deleteManifest(tx, referrerDigest)
		if err != nil {
			if errors.Is(err, store.ErrManifestInUse) {
				continue
			}
			return deleted, err
		}
		deleted = append(deleted, digests...)
	}

	manifests.removeManifest(id)

	blobs.blob(id).removeOwner(id)
	if err := r.deleteBlob(tx, id); err != nil {
		return nil, err
	}

	deleted = append(deleted, id)
	return deleted, nil
}

func (r *repositoryStore) ListReferrers(subject digest.Digest) ([]digest.Digest, error) {
	refs := make([]digest.Digest, 0)

	_ = r.db.View(func(tx *bolt.Tx) error {
		refs = slices.Collect(
			r.repository(tx).manifests().manifest(subject).referrers(),
		)
		return nil
	})
	return refs, nil
}

func (r *repositoryStore) ListTags(count int, last string) ([]string, error) {
	result := make([]string, 0)

	err := r.db.View(func(tx *bolt.Tx) error {
		tags := r.repository(tx).tags()
		c := tags.b.Cursor()

		k, _ := c.Seek([]byte(last))
		if last != "" {
			if tag := tags.tag(last); tag == "" {
				return fmt.Errorf("%w: %s", store.ErrTagNotFound, tag)
			}
			// If 'last' is not empty, we should start at the tag after the last one.
			k, _ = c.Next()
		}
		for i := 0; k != nil && (count == -1 || i < count); i++ {
			result = append(result, string(k))
			k, _ = c.Next()
		}

		return nil
	})

	return result, err
}

func (r *repositoryStore) GetTag(tag string) (digest.Digest, error) {
	var id digest.Digest
	err := r.db.View(func(tx *bolt.Tx) error {
		id = r.repository(tx).tags().tag(tag)
		if id == "" {
			return fmt.Errorf("%w: %s", store.ErrTagNotFound, tag)
		}
		return nil
	})

	return id, err
}

func (r *repositoryStore) PutTag(tag string, id digest.Digest) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		repo := r.repository(tx)
		manifest := repo.manifests().manifest(id)
		if !manifest.found() {
			return fmt.Errorf("%w: %s", store.ErrManifestNotFound, id)
		}

		manifest.addTagOwner(tag)
		repo.tags().addTag(tag, id)
		return nil
	})
}

func (r *repositoryStore) DeleteTag(tag string) ([]digest.Digest, error) {
	var deleted []digest.Digest

	err := r.db.Update(func(tx *bolt.Tx) error {
		repo := r.repository(tx)
		id := repo.tags().tag(tag)
		if id == "" {
			return fmt.Errorf("%w: %s", store.ErrTagNotFound, tag)
		}

		repo.tags().removeTag(tag)
		repo.manifests().manifest(id).removeTagOwner(tag)

		var err error
		deleted, err = r.deleteManifest(tx, id)
		return err
	})

	return deleted, err
}

func (r *repositoryStore) GetUploadSession(id uuid.UUID) (*store.UploadSession, error) {
	var upload *store.UploadSession
	err := r.db.View(func(tx *bolt.Tx) error {
		upload = r.repository(tx).uploads().upload(id)
		if upload == nil {
			return fmt.Errorf("%w: %s", store.ErrUploadNotFound, id)
		}
		return nil
	})
	return upload, err
}

func (r *repositoryStore) PutUploadSession(session *store.UploadSession) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		r.repository(tx).uploads().putUpload(session)
		return nil
	})
}

func (r *repositoryStore) DeleteUploadSession(id uuid.UUID) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		upload := r.repository(tx).uploads().upload(id)
		if upload == nil {
			return fmt.Errorf("%w: %s", store.ErrUploadNotFound, id)
		}
		r.repository(tx).uploads().removeUpload(id)
		return nil
	})
}

func (r *repositoryStore) blobs(tx *bolt.Tx) sharedBlobs {
	return sharedBlobs{
		b: tx.Bucket(_BLOBS),
	}
}

func (r *repositoryStore) repository(tx *bolt.Tx) repository {
	return repository{
		b: tx.Bucket(_REPOSITORIES).Bucket([]byte(r.name)),
	}
}
