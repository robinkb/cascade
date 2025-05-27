package boltdb

import (
	"bytes"
	"encoding/gob"
	"path/filepath"

	"github.com/robinkb/cascade-registry/store"

	"github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"
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
	path := filepath.Join(baseDir, "bolt.db")

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		panic(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(_REPOSITORIES)
		return err
	}); err != nil {
		panic(err)
	}

	return &metadataStore{
		db: db,
	}
}

type metadataStore struct {
	store.Metadata
	db *bolt.DB
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

func (s *metadataStore) GetBlob(name string, digest digest.Digest) (string, error) {
	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		blobs := repo.Bucket(_BLOBS)
		if blobs == nil {
			return store.ErrNotFound
		}

		blob := blobs.Get([]byte(digest.String()))
		if blob == nil {
			return store.ErrNotFound
		}

		return nil
	})

	return "", err
}

func (s *metadataStore) PutBlob(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo, err := tx.Bucket(_REPOSITORIES).CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return store.ErrNotFound
		}

		blobs, err := repo.CreateBucketIfNotExists(_BLOBS)
		if err != nil {
			return store.ErrNotFound
		}

		return blobs.Put([]byte(digest.String()), []byte{})
	})
}

func (s *metadataStore) DeleteBlob(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		blobs := repo.Bucket(_BLOBS)
		if blobs == nil {
			return store.ErrNotFound
		}

		return blobs.Delete([]byte(digest.String()))
	})
}

func (s *metadataStore) GetManifest(name string, digest digest.Digest) (*store.ManifestMetadata, error) {
	var buf *bytes.Buffer
	var meta manifestMetadata

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		manifests := repo.Bucket(_MANIFESTS)
		if manifests == nil {
			return store.ErrNotFound
		}

		manifest := manifests.Bucket([]byte(digest.String()))
		if manifest == nil {
			return store.ErrNotFound
		}

		meta := manifest.Get(_METADATA)
		if meta == nil {
			return store.ErrNotFound
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
		repo, err := tx.Bucket(_REPOSITORIES).CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}

		manifests, err := repo.CreateBucketIfNotExists(_MANIFESTS)
		if err != nil {
			return err
		}

		manifest, err := manifests.CreateBucketIfNotExists([]byte(digest.String()))
		if err != nil {
			return err
		}

		return manifest.Put(_METADATA, buf.Bytes())
	})
}

func (s *metadataStore) DeleteManifest(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		manifests := repo.Bucket(_MANIFESTS)
		if manifests == nil {
			return store.ErrNotFound
		}

		return manifests.DeleteBucket([]byte(digest.String()))
	})
}

func (s *metadataStore) ListTags(name string, count int, last string) ([]string, error) {
	result := make([]string, 0)

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		tags := repo.Bucket(_TAGS)
		if tags == nil {
			return store.ErrNotFound
		}

		cursor := tags.Cursor()
		// If 'last' is empty, the cursor is placed at the first key.
		k, _ := cursor.Seek([]byte(last))
		if last != "" {
			// If 'last' is not empty, we should start at the tag after the last one.
			k, _ = cursor.Next()
		}
		for i := 0; k != nil && (count == -1 || i < count); i++ {
			result = append(result, string(k))
			k, _ = cursor.Next()
		}

		return nil
	})

	return result, err
}

func (s *metadataStore) GetTag(name string, tag string) (digest.Digest, error) {
	var buf *bytes.Buffer
	var meta tagMetadata

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		tags := repo.Bucket(_TAGS)
		if tags == nil {
			return store.ErrNotFound
		}

		content := tags.Get([]byte(tag))
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
		repo, err := tx.Bucket(_REPOSITORIES).CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return store.ErrNotFound
		}

		tags, err := repo.CreateBucketIfNotExists(_TAGS)
		if err != nil {
			return store.ErrNotFound
		}

		return tags.Put([]byte(tag), buf.Bytes())
	})
}

func (s *metadataStore) DeleteTag(name string, tag string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		tags := repo.Bucket(_TAGS)
		if tags == nil {
			return store.ErrNotFound
		}

		return tags.Delete([]byte(tag))
	})
}

func (s *metadataStore) ListReferrers(name string, subject digest.Digest) ([]digest.Digest, error) {
	refs := make([]digest.Digest, 0)

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		manifests := repo.Bucket(_MANIFESTS)
		if manifests == nil {
			return store.ErrNotFound
		}

		manifest := repo.Bucket([]byte(subject))
		if manifest == nil {
			return nil
		}

		referrers := manifest.Bucket(_REFERRERS)
		if referrers == nil {
			return store.ErrNotFound
		}

		c := referrers.Cursor()
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
			return store.ErrNotFound
		}

		uploads := repo.Bucket(_UPLOADS)
		if uploads == nil {
			return store.ErrNotFound
		}

		content := uploads.Get([]byte(id))
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
		repo, err := tx.Bucket(_REPOSITORIES).CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return store.ErrNotFound
		}

		uploads, err := repo.CreateBucketIfNotExists(_UPLOADS)
		if err != nil {
			return store.ErrNotFound
		}

		return uploads.Put([]byte(session.ID.String()), buf.Bytes())
	})
}

func (s *metadataStore) DeleteUploadSession(name string, id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket(_REPOSITORIES).Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		uploads := repo.Bucket(_UPLOADS)
		if uploads == nil {
			return store.ErrNotFound
		}

		return uploads.Delete([]byte(id))
	})
}
