package cluster

import (
	"math/rand/v2"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/cluster"
	"github.com/robinkb/cascade-registry/store"
)

func NewMetadataStore(node cluster.Node, metadata store.Metadata) store.Metadata {
	s := &metadataStore{
		Metadata: metadata,
		node:     node,
	}

	node.Process(&createRepository{}, s.createRepository)
	node.Process(&deleteRepository{}, s.deleteRepository)
	node.Process(&putBlobMeta{}, s.putBlob)
	node.Process(&deleteBlobMeta{}, s.deleteBlob)
	node.Process(&putManifest{}, s.putManifest)
	node.Process(&deleteManifest{}, s.deleteManifest)
	node.Process(&putTag{}, s.putTag)
	node.Process(&deleteTag{}, s.deleteTag)
	node.Process(&putUploadSession{}, s.putUploadSession)
	node.Process(&deleteUploadSession{}, s.deleteUploadSession)

	return s
}

type metadataStore struct {
	store.Metadata
	node cluster.Node
}

func (s *metadataStore) CreateRepository(name string) error {
	op := &createRepository{
		rand.Uint64(),
		name,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) createRepository(op cluster.Operation) error {
	v := op.(*createRepository)
	return s.Metadata.CreateRepository(v.Name)
}

func (s *metadataStore) DeleteRepository(name string) error {
	op := &deleteRepository{
		rand.Uint64(),
		name,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) deleteRepository(op cluster.Operation) error {
	v := op.(*deleteRepository)
	return s.Metadata.DeleteRepository(v.Name)
}

func (s *metadataStore) PutBlob(name string, digest digest.Digest) error {
	op := &putBlobMeta{
		rand.Uint64(),
		name, digest,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) putBlob(op cluster.Operation) error {
	v := op.(*putBlobMeta)
	return s.Metadata.PutBlob(v.Name, v.Digest)
}

func (s *metadataStore) DeleteBlob(name string, digest digest.Digest) error {
	op := &deleteBlobMeta{
		rand.Uint64(),
		name, digest,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) deleteBlob(op cluster.Operation) error {
	v := op.(*deleteBlobMeta)
	return s.Metadata.DeleteBlob(v.Name, v.Digest)
}

func (s *metadataStore) PutManifest(name string, digest digest.Digest, meta *store.ManifestMetadata) error {
	op := &putManifest{
		rand.Uint64(),
		name, digest, meta,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) putManifest(op cluster.Operation) error {
	v := op.(*putManifest)
	return s.Metadata.PutManifest(v.Name, v.Digest, v.Meta)
}

func (s *metadataStore) DeleteManifest(name string, digest digest.Digest) error {
	op := &deleteManifest{
		rand.Uint64(),
		name, digest,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) deleteManifest(op cluster.Operation) error {
	v := op.(*deleteManifest)
	return s.Metadata.DeleteManifest(v.Name, v.Digest)
}

func (s *metadataStore) PutTag(name, tag string, digest digest.Digest) error {
	op := &putTag{
		rand.Uint64(),
		name, tag, digest,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) putTag(op cluster.Operation) error {
	v := op.(*putTag)
	return s.Metadata.PutTag(v.Name, v.Tag, v.Digest)
}

func (s *metadataStore) DeleteTag(name, tag string) error {
	op := &deleteTag{
		rand.Uint64(),
		name, tag,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) deleteTag(op cluster.Operation) error {
	v := op.(*deleteTag)
	return s.Metadata.DeleteTag(v.Name, v.Tag)
}

func (s *metadataStore) PutUploadSession(name string, session *store.UploadSession) error {
	op := &putUploadSession{
		rand.Uint64(),
		name, session,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) putUploadSession(op cluster.Operation) error {
	v := op.(*putUploadSession)
	return s.Metadata.PutUploadSession(v.Name, v.Session)
}

func (s *metadataStore) DeleteUploadSession(name string, id string) error {
	op := &deleteUploadSession{
		rand.Uint64(),
		name, id,
	}
	return s.node.Propose(op)
}

func (s *metadataStore) deleteUploadSession(op cluster.Operation) error {
	v := op.(*deleteUploadSession)
	return s.Metadata.DeleteUploadSession(v.Name, v.SessionID)
}
