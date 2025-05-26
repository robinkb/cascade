package boltdb

import (
	"bytes"
	"encoding/gob"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/store"
	bolt "go.etcd.io/bbolt"
)

func NewMetadataStore() *metadataStore {
	db, err := bolt.Open("my.db", 0600, nil)
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
		blobs, err := s.blobsForRepo(tx, name)
		if err != nil {
			return err
		}

		blob := blobs.Get([]byte(digest))
		if blob == nil {
			return store.ErrNotFound
		}

		return nil
	})

	return "", err
}

func (s *metadataStore) PutBlob(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		blobs, err := s.blobsForRepo(tx, name)
		if err != nil {
			return err
		}

		return blobs.Put([]byte(digest.String()), nil)
	})
}

func (s *metadataStore) DeleteBlob(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		blobs, err := s.blobsForRepo(tx, name)
		if err != nil {
			return err
		}

		return blobs.Delete([]byte(digest.String()))
	})
}

func (s *metadataStore) GetManifest(name string, digest digest.Digest) (*store.ManifestMetadata, error) {
	var buf *bytes.Buffer
	var meta manifestMetadata

	err := s.db.View(func(tx *bolt.Tx) error {
		manifests, err := s.manifestsForRepo(tx, name)
		if err != nil {
			return err
		}

		content := manifests.Get([]byte(digest))
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
		manifests, err := s.manifestsForRepo(tx, name)
		if err != nil {
			return err
		}

		return manifests.Put([]byte(digest.String()), buf.Bytes())
	})
}

func (s *metadataStore) DeleteManifest(name string, digest digest.Digest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		manifests, err := s.manifestsForRepo(tx, name)
		if err != nil {
			return err
		}

		return manifests.Delete([]byte(digest.String()))
	})
}

func (s *metadataStore) ListTags(repository string, count int, last string) ([]string, error) {
	return []string{}, nil
}

func (s *metadataStore) GetTag(name string, tag string) (digest.Digest, error) {
	var buf *bytes.Buffer
	var meta tagMetadata

	err := s.db.View(func(tx *bolt.Tx) error {
		tags, err := s.tagsForRepo(tx, name)
		if err != nil {
			return err
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
		tags, err := s.tagsForRepo(tx, name)
		if err != nil {
			return err
		}

		return tags.Put([]byte(tag), buf.Bytes())
	})
}

func (s *metadataStore) DeleteTag(name string, tag string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		tags, err := s.tagsForRepo(tx, name)
		if err != nil {
			return err
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
		uploads, err := s.uploadsForRepo(tx, name)
		if err != nil {
			return err
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
		uploads, err := s.uploadsForRepo(tx, name)
		if err != nil {
			return err
		}

		return uploads.Put([]byte(session.ID.String()), buf.Bytes())
	})
}

func (s *metadataStore) DeleteUploadSession(name string, id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		uploads, err := s.uploadsForRepo(tx, name)
		if err != nil {
			return err
		}

		return uploads.Delete([]byte(id))
	})
}

func (s *metadataStore) blobsForRepo(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	repo, err := s.createOrGetRepo(tx, name)
	if err != nil {
		return nil, err
	}

	return repo.Bucket([]byte("blobs")), nil
}

func (s *metadataStore) manifestsForRepo(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	repo, err := s.createOrGetRepo(tx, name)
	if err != nil {
		return nil, err
	}

	return repo.Bucket([]byte("manifests")), nil
}

func (s *metadataStore) tagsForRepo(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	repo, err := s.createOrGetRepo(tx, name)
	if err != nil {
		return nil, err
	}

	return repo.Bucket([]byte("tags")), nil
}

func (s *metadataStore) uploadsForRepo(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	repo, err := s.createOrGetRepo(tx, name)
	if err != nil {
		return nil, err
	}

	return repo.Bucket([]byte("uploads")), nil
}

func (s *metadataStore) createOrGetRepo(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	var repo *bolt.Bucket
	var err error

	repo = tx.Bucket([]byte(name))
	if repo == nil {
		if !tx.Writable() {
			return nil, store.ErrNotFound
		}
	}

	repo, err = tx.CreateBucket([]byte(name))
	if err != nil {
		return nil, err
	}

	for _, bucket := range []string{"blobs", "manifests", "tags", "uploads"} {
		_, err := tx.CreateBucket([]byte(bucket))
		if err != nil {
			return nil, err
		}
	}

	return repo, err
}
