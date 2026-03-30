package boltdb

import (
	"bytes"
	"encoding/gob"
	"iter"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/registry/store"
	bolt "go.etcd.io/bbolt"
)

// Types for wrapping BoltDB queries to codify the database structure.
// sharedBlobs contains the metadata of all the blobs in the blob store.
type sharedBlobs struct {
	b *bolt.Bucket
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
	b *bolt.Bucket
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
	b *bolt.Bucket
}

func (o repository) blobs() repoBlobs {
	return repoBlobs{o.b.Bucket(_BLOBS)}
}

func (o repository) manifests() manifests {
	return manifests{o.b.Bucket(_MANIFESTS)}
}

func (o repository) tags() tags {
	return tags{o.b.Bucket(_TAGS)}
}

func (o repository) uploads() uploads {
	return uploads{o.b.Bucket(_UPLOADS)}
}

// repoBlobs contains metadata of blobs belonging to a specific repository.
type repoBlobs struct {
	b *bolt.Bucket
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
	b *bolt.Bucket
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
	b *bolt.Bucket
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
	b *bolt.Bucket
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

func (o manifest) referrers() iter.Seq[digest.Digest] {
	return func(yield func(digest.Digest) bool) {
		o.b.Bucket(_REFERRERS).ForEach(func(id, _ []byte) error {
			if !yield(digest.Digest(id)) {
				return nil
			}
			return nil
		})
	}
}

func (o manifest) addReferrer(id digest.Digest) {
	o.b.Bucket(_REFERRERS).Put([]byte(id), nil)
}

func (o manifest) removeReferrer(id digest.Digest) {
	o.b.Bucket(_REFERRERS).Delete([]byte(id))
}

func (o manifest) addManifestOwner(id digest.Digest) {
	o.b.Bucket(_MANIFESTS).Put([]byte(id), nil)
}

func (o manifest) removeManifestOwner(id digest.Digest) {
	o.b.Bucket(_MANIFESTS).Delete([]byte(id))
}

func (o manifest) addTagOwner(tag string) {
	o.b.Bucket(_TAGS).Put([]byte(tag), nil)
}

func (o manifest) removeTagOwner(tag string) {
	o.b.Bucket(_TAGS).Delete([]byte(tag))
}

func (o manifest) hasOwners() bool {
	return o.b.Bucket(_MANIFESTS).Inspect().KeyN != 0 ||
		o.b.Bucket(_TAGS).Inspect().KeyN != 0
}

type tags struct {
	b *bolt.Bucket
}

func (o tags) tag(t string) digest.Digest {
	return digest.Digest(o.b.Get([]byte(t)))
}

func (o tags) addTag(t string, id digest.Digest) {
	o.b.Put([]byte(t), []byte(id))
}

func (o tags) removeTag(t string) {
	o.b.Delete([]byte(t))
}

type uploads struct {
	b *bolt.Bucket
}

func (o uploads) upload(id uuid.UUID) *store.UploadSession {
	data := o.b.Get(id.Bytes())
	if data == nil {
		return nil
	}
	var upload *store.UploadSession
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(&upload); err != nil {
		panic(err)
	}
	return upload
}

func (o uploads) putUpload(upload *store.UploadSession) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(upload); err != nil {
		panic(err)
	}
	o.b.Put(upload.ID.Bytes(), buf.Bytes())
}

func (o uploads) removeUpload(id uuid.UUID) {
	o.b.Delete(id.Bytes())
}
