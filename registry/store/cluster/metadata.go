package cluster

import (
	"math/rand/v2"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry/cluster"
	"github.com/robinkb/cascade-registry/registry/store"
)

func NewMetadataStore(proposer cluster.Proposer, metadata store.Metadata) store.Metadata {
	s := &metadataStore{
		Metadata: metadata,
		proposer: proposer,
	}

	proposer.Handle(&createRepository{}, s.createRepository)
	proposer.Handle(&deleteRepository{}, s.deleteRepository)
	proposer.Handle(&putBlobMeta{}, s.putBlob)
	proposer.Handle(&deleteBlobMeta{}, s.deleteBlob)
	proposer.Handle(&putManifest{}, s.putManifest)
	proposer.Handle(&deleteManifest{}, s.deleteManifest)
	proposer.Handle(&putTag{}, s.putTag)
	proposer.Handle(&deleteTag{}, s.deleteTag)
	proposer.Handle(&putUploadSession{}, s.putUploadSession)
	proposer.Handle(&deleteUploadSession{}, s.deleteUploadSession)

	return s
}

type metadataStore struct {
	store.Metadata
	proposer cluster.Proposer
}

func (s *metadataStore) CreateRepository(name string) error {
	p := &createRepository{
		rand.Uint64(),
		name,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) createRepository(p cluster.Proposal) error {
	v := p.(*createRepository)
	return s.Metadata.CreateRepository(v.Name)
}

func (s *metadataStore) DeleteRepository(name string) error {
	p := &deleteRepository{
		rand.Uint64(),
		name,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) deleteRepository(p cluster.Proposal) error {
	v := p.(*deleteRepository)
	return s.Metadata.DeleteRepository(v.Name)
}

func (s *metadataStore) PutBlob(name string, digest digest.Digest) error {
	p := &putBlobMeta{
		rand.Uint64(),
		name, digest,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) putBlob(p cluster.Proposal) error {
	v := p.(*putBlobMeta)
	return s.Metadata.PutBlob(v.Name, v.Digest)
}

func (s *metadataStore) DeleteBlob(name string, digest digest.Digest) error {
	p := &deleteBlobMeta{
		rand.Uint64(),
		name, digest,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) deleteBlob(p cluster.Proposal) error {
	v := p.(*deleteBlobMeta)
	return s.Metadata.DeleteBlob(v.Name, v.Digest)
}

func (s *metadataStore) PutManifest(name string, digest digest.Digest, meta *store.ManifestMetadata) error {
	p := &putManifest{
		rand.Uint64(),
		name, digest, meta,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) putManifest(p cluster.Proposal) error {
	v := p.(*putManifest)
	return s.Metadata.PutManifest(v.Name, v.Digest, v.Meta)
}

func (s *metadataStore) DeleteManifest(name string, digest digest.Digest) error {
	p := &deleteManifest{
		rand.Uint64(),
		name, digest,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) deleteManifest(p cluster.Proposal) error {
	v := p.(*deleteManifest)
	return s.Metadata.DeleteManifest(v.Name, v.Digest)
}

func (s *metadataStore) PutTag(name, tag string, digest digest.Digest) error {
	p := &putTag{
		rand.Uint64(),
		name, tag, digest,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) putTag(p cluster.Proposal) error {
	v := p.(*putTag)
	return s.Metadata.PutTag(v.Name, v.Tag, v.Digest)
}

func (s *metadataStore) DeleteTag(name, tag string) error {
	p := &deleteTag{
		rand.Uint64(),
		name, tag,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) deleteTag(p cluster.Proposal) error {
	v := p.(*deleteTag)
	return s.Metadata.DeleteTag(v.Name, v.Tag)
}

func (s *metadataStore) PutUploadSession(name string, session *store.UploadSession) error {
	p := &putUploadSession{
		rand.Uint64(),
		name, session,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) putUploadSession(p cluster.Proposal) error {
	v := p.(*putUploadSession)
	return s.Metadata.PutUploadSession(v.Name, v.Session)
}

func (s *metadataStore) DeleteUploadSession(name string, id string) error {
	p := &deleteUploadSession{
		rand.Uint64(),
		name, id,
	}
	return s.proposer.Propose(p)
}

func (s *metadataStore) deleteUploadSession(p cluster.Proposal) error {
	v := p.(*deleteUploadSession)
	return s.Metadata.DeleteUploadSession(v.Name, v.SessionID)
}
