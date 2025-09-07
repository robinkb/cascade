package boltdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/robinkb/cascade-registry/store"

	"github.com/opencontainers/go-digest"
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
)

func NewMetadataStore(baseDir string) store.Metadata {
	path := filepath.Join(baseDir, "metadata.db")

	opts := &bolt.Options{
		Timeout: 1 * time.Second,
	}
	db, err := bolt.Open(path, 0600, opts)
	if err != nil {
		panic(err)
	}

	// Initialize top-level buckets that hold all repositories,
	// and cross-repository blob references.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(_REPOSITORIES)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(_BLOBS)
		return err
	}); err != nil {
		panic(err)
	}

	return &metadataStore{
		db:   db,
		opts: opts,
	}
}

type metadataStore struct {
	db   *bolt.DB
	opts *bolt.Options
}

type manifestMetadata struct {
	Annotations  map[string]string
	ArtifactType string
	MediaType    string
	Size         int64
}

type tagMetadata struct {
	Digest digest.Digest
}

func (s *metadataStore) CreateRepository(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repos := tx.Bucket(_REPOSITORIES)

		repo, err := repos.CreateBucket([]byte(name))
		// CreateRepository may be called concurrently, in which case
		// the bucket may already be in the process of creation
		// by another thread.
		if err != nil {
			if errors.Is(err, bolterrors.ErrBucketExists) {
				return nil
			}
			return err
		}

		for _, bucket := range [][]byte{_BLOBS, _MANIFESTS, _TAGS, _UPLOADS} {
			_, err = repo.CreateBucket(bucket)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *metadataStore) GetRepository(name string) error {
	return s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrRepositoryNotFound
		}
		return nil
	})
}

func (s *metadataStore) DeleteRepository(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(_REPOSITORIES).DeleteBucket([]byte(name))
	})
}

func (s *metadataStore) ListBlobs() ([]digest.Digest, error) {
	digests := make([]digest.Digest, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(_BLOBS).Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			id, err := digest.Parse(string(k))
			if err != nil {
				return err
			}
			digests = append(digests, id)
		}

		return nil
	})

	return digests, err
}

func (s *metadataStore) GetBlob(name string, digest digest.Digest) (string, error) {
	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		blob := repo.Bucket(_BLOBS).Get([]byte(digest.String()))
		if blob == nil {
			return store.ErrNotFound
		}

		return nil
	})

	return "", err
}

func (s *metadataStore) PutBlob(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		err := repo.Bucket(_BLOBS).Put([]byte(digest.String()), []byte{})
		if err != nil {
			return err
		}

		return tx.Bucket(_BLOBS).Put([]byte(digest.String()), []byte{})
	})
}

func (s *metadataStore) DeleteBlob(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		err := repo.Bucket(_BLOBS).Delete([]byte(digest.String()))
		if err != nil {
			return err
		}

		return tx.Bucket(_BLOBS).Delete([]byte(digest.String()))
	})
}

func (s *metadataStore) GetManifest(name string, digest digest.Digest) (*store.ManifestMetadata, error) {
	var buf *bytes.Buffer
	var meta manifestMetadata

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrRepositoryNotFound
		}

		manifest := repo.Bucket(_MANIFESTS).Bucket([]byte(digest.String()))
		if manifest == nil {
			return store.ErrMetadataNotFound
		}

		meta := manifest.Get(_METADATA)
		if meta == nil {
			return store.ErrMetadataNotFound
		}
		buf = bytes.NewBuffer(meta)
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = gob.NewDecoder(buf).Decode(&meta)
	if err != nil {
		return nil, err
	}

	return &store.ManifestMetadata{
		Annotations:  meta.Annotations,
		ArtifactType: meta.ArtifactType,
		MediaType:    meta.MediaType,
		Size:         meta.Size,
	}, nil
}

func (s *metadataStore) PutManifest(name string, digest digest.Digest, metadata *store.ManifestMetadata) error {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(&manifestMetadata{
		Annotations:  metadata.Annotations,
		ArtifactType: metadata.ArtifactType,
		MediaType:    metadata.MediaType,
		Size:         metadata.Size,
	})
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrRepositoryNotFound
		}

		manifest, err := s.createManifestBucketIfNotExists(repo.Bucket(_MANIFESTS), digest)
		if err != nil {
			return err
		}

		err = manifest.Put(_METADATA, buf.Bytes())
		if err != nil {
			return err
		}

		if metadata.Subject != "" {
			subject, err := s.createManifestBucketIfNotExists(repo.Bucket(_MANIFESTS), metadata.Subject)
			if err != nil {
				return err
			}

			return subject.Bucket(_REFERRERS).Put([]byte(digest.String()), []byte{})
		}

		return nil
	})
}

func (s *metadataStore) DeleteManifest(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		return repo.Bucket(_MANIFESTS).DeleteBucket([]byte(digest.String()))
	})
}

func (s *metadataStore) ListTags(name string, count int, last string) ([]string, error) {
	tags := make([]string, 0)

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		cursor := repo.Bucket(_TAGS).Cursor()
		// If 'last' is empty, the cursor is placed at the first key.
		k, _ := cursor.Seek([]byte(last))
		if last != "" {
			// If 'last' is not empty, we should start at the tag after the last one.
			k, _ = cursor.Next()
		}
		for i := 0; k != nil && (count == -1 || i < count); i++ {
			tags = append(tags, string(k))
			k, _ = cursor.Next()
		}

		return nil
	})

	return tags, err
}

func (s *metadataStore) GetTag(name string, tag string) (digest.Digest, error) {
	var buf *bytes.Buffer
	var meta tagMetadata

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		content := repo.Bucket(_TAGS).Get([]byte(tag))
		if content == nil {
			return store.ErrNotFound
		}
		buf = bytes.NewBuffer(content)
		return nil
	})
	if err != nil {
		return "", err
	}

	err = gob.NewDecoder(buf).Decode(&meta)
	if err != nil {
		return "", err
	}

	return meta.Digest, nil
}

func (s *metadataStore) PutTag(name string, tag string, digest digest.Digest) error {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(&tagMetadata{
		Digest: digest,
	})
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrRepositoryNotFound
		}

		return repo.Bucket(_TAGS).Put([]byte(tag), buf.Bytes())
	})
}

func (s *metadataStore) DeleteTag(name string, tag string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		return repo.Bucket(_TAGS).Delete([]byte(tag))
	})
}

func (s *metadataStore) ListReferrers(name string, subject digest.Digest) ([]digest.Digest, error) {
	refs := make([]digest.Digest, 0)

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		manifest := repo.Bucket(_MANIFESTS).Bucket([]byte(subject))
		if manifest == nil {
			return nil
		}

		c := manifest.Bucket(_REFERRERS).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			d, err := digest.Parse(string(k))
			if err != nil {
				return err
			}
			refs = append(refs, d)
		}

		return nil
	})

	return refs, err
}

func (s *metadataStore) GetUploadSession(name string, id string) (*store.UploadSession, error) {
	var buf *bytes.Buffer
	var session store.UploadSession

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrRepositoryNotFound
		}

		content := repo.Bucket(_UPLOADS).Get([]byte(id))
		if content == nil {
			return store.ErrNotFound
		}
		buf = bytes.NewBuffer(content)
		return nil
	})
	if err != nil {
		return nil, err
	}

	err = gob.NewDecoder(buf).Decode(&session)
	return &session, err
}

func (s *metadataStore) PutUploadSession(name string, session *store.UploadSession) error {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(session)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrRepositoryNotFound
		}

		return repo.Bucket(_UPLOADS).Put([]byte(session.ID.String()), buf.Bytes())
	})
}

func (s *metadataStore) DeleteUploadSession(name string, id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		return repo.Bucket(_UPLOADS).Delete([]byte(id))
	})
}

func (s *metadataStore) Snapshot(w io.Writer) error {
	return s.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

func (s *metadataStore) Restore(r io.Reader) error {
	path := s.db.Path()
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

	err = s.db.Close()
	if err != nil {
		return err
	}
	err = os.Rename(tmp, path)
	if err != nil {
		return err
	}
	db, err := bolt.Open(path, 0600, s.opts)
	if err != nil {
		return err
	}

	s.db = db
	return nil
}

func (s *metadataStore) createManifestBucketIfNotExists(bucket *bolt.Bucket, digest digest.Digest) (*bolt.Bucket, error) {
	manifest, err := bucket.CreateBucketIfNotExists([]byte(digest.String()))
	if err != nil {
		return nil, err
	}

	_, err = manifest.CreateBucketIfNotExists(_REFERRERS)
	if err != nil {
		return nil, err
	}

	return manifest, err
}
