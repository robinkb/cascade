package boltdb

import (
	"bytes"
	"encoding/gob"
	"path/filepath"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
	bolt "go.etcd.io/bbolt"
)

func NewMetadataStore(baseDir string) *metadataStore {
	path := filepath.Join(baseDir, "bolt.db")

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
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
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		blobs := repo.Bucket([]byte("blobs"))
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
		repo, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return store.ErrNotFound
		}

		blobs, err := repo.CreateBucketIfNotExists([]byte("blobs"))
		if err != nil {
			return store.ErrNotFound
		}

		return blobs.Put([]byte(digest.String()), []byte{})
	})
}

func (s *metadataStore) DeleteBlob(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		blobs := repo.Bucket([]byte("blobs"))
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
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		manifests := repo.Bucket([]byte("manifests"))
		if manifests == nil {
			return store.ErrNotFound
		}

		content := manifests.Get([]byte(digest.String()))
		if content == nil {
			return store.ErrNotFound
		}
		buf = bytes.NewBuffer(content)
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

func (s *metadataStore) PutManifest(name string, digest digest.Digest, meta *store.ManifestMetadata) error {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(&manifestMetadata{
		Annotations:  meta.Annotations,
		ArtifactType: meta.ArtifactType,
		MediaType:    meta.MediaType,
		Size:         meta.Size,
	})
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		repo, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return store.ErrNotFound
		}

		manifests, err := repo.CreateBucketIfNotExists([]byte("manifests"))
		if err != nil {
			return store.ErrNotFound
		}

		return manifests.Put([]byte(digest.String()), buf.Bytes())
	})
}

func (s *metadataStore) DeleteManifest(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		manifests := repo.Bucket([]byte("manifests"))
		if manifests == nil {
			return store.ErrNotFound
		}

		return manifests.Delete([]byte(digest.String()))
	})
}

func (s *metadataStore) ListTags(name string, count int, last string) ([]string, error) {
	result := make([]string, 0)

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		tags := repo.Bucket([]byte("tags"))
		if tags == nil {
			return store.ErrNotFound
		}

		cursor := tags.Cursor()
		k, _ := cursor.Seek([]byte(last))
		if last != "" {
			k, _ = cursor.Next()
		}
		i := 0
		for {
			if k == nil {
				break
			}

			if count != -1 && i >= count {
				break
			}

			result = append(result, string(k))
			i++

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
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		tags := repo.Bucket([]byte("tags"))
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
		repo, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return store.ErrNotFound
		}

		tags, err := repo.CreateBucketIfNotExists([]byte("tags"))
		if err != nil {
			return store.ErrNotFound
		}

		return tags.Put([]byte(tag), buf.Bytes())
	})
}

func (s *metadataStore) DeleteTag(name string, tag string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		tags := repo.Bucket([]byte("tags"))
		if tags == nil {
			return store.ErrNotFound
		}

		return tags.Delete([]byte(tag))
	})
}

func (s *metadataStore) ListReferrers(name string, digest digest.Digest) ([]digest.Digest, error) {
	return nil, nil
}

func (s *metadataStore) GetUploadSession(name string, id string) (*store.UploadSession, error) {
	var buf *bytes.Buffer
	var session store.UploadSession

	err := s.db.View(func(tx *bolt.Tx) error {
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		uploads := repo.Bucket([]byte("uploads"))
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
		repo, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return store.ErrNotFound
		}

		uploads, err := repo.CreateBucketIfNotExists([]byte("uploads"))
		if err != nil {
			return store.ErrNotFound
		}

		return uploads.Put([]byte(session.ID.String()), buf.Bytes())
	})
}

func (s *metadataStore) DeleteUploadSession(name string, id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		repo := tx.Bucket([]byte(name))
		if repo == nil {
			return store.ErrNotFound
		}

		uploads := repo.Bucket([]byte("uploads"))
		if uploads == nil {
			return store.ErrNotFound
		}

		return uploads.Delete([]byte(id))
	})
}
