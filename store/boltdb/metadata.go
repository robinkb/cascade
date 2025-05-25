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

func (s *metadataStore) DeleteManifest(repository string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) ListTags(repository string, count int, last string) ([]string, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) GetTag(repository string, tag string) (digest.Digest, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) PutTag(repository string, tag string, digest digest.Digest) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteTag(repository string, tag string) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) ListReferrers(repository string, digest digest.Digest) ([]digest.Digest, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) GetUploadSession(repository string, id string) (*store.UploadSession, error) {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) PutUploadSession(repository string, session *store.UploadSession) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) DeleteUploadSession(repository string, id string) error {
	panic("not implemented") // TODO: Implement
}

func (s *metadataStore) blobsForRepo(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	repo, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}

	return repo.CreateBucketIfNotExists([]byte("blobs"))
}

func (s *metadataStore) manifestsForRepo(tx *bolt.Tx, name string) (*bolt.Bucket, error) {
	repo, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}

	return repo.CreateBucketIfNotExists([]byte("manifests"))
}
