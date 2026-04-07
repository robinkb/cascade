package cluster

import (
	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/registry/store"
)

func NewMetadataStore(proposer cluster.Proposer, meta store.Metadata) store.Metadata {
	s := &metadataStore{
		Metadata: meta,
		proposer: proposer,
	}

	proposer.Handle(&pCreateRepository{}, s.createRepository)
	proposer.Handle(&pDeleteRepository{}, s.deleteRepository)
	proposer.Handle(&pPutBlobMeta{}, s.putBlob)
	proposer.Handle(&pDeleteBlobMeta{}, s.deleteBlob)
	proposer.Handle(&pPutManifest{}, s.putManifest)
	proposer.Handle(&pDeleteManifest{}, s.deleteManifest)
	proposer.Handle(&pPutTag{}, s.putTag)
	proposer.Handle(&pDeleteTag{}, s.deleteTag)
	proposer.Handle(&pPutUploadSession{}, s.putUploadSession)
	proposer.Handle(&pDeleteUploadSession{}, s.deleteUploadSession)

	return s
}

type metadataStore struct {
	store.Metadata
	proposer cluster.Proposer
}

type pCreateRepository struct {
	cluster.ProposalBase
	Name string
}

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	p := &pCreateRepository{
		Name: name,
	}
	resp := m.proposer.Propose(p)
	if resp != nil {
		return nil, resp
	}

	return m.GetRepository(name)
}

func (m *metadataStore) createRepository(p cluster.Proposal) cluster.Response {
	v := p.(*pCreateRepository)
	_, err := m.Metadata.CreateRepository(v.Name)
	return err
}

func (m *metadataStore) GetRepository(name string) (store.Repository, error) {
	repo, err := m.Metadata.GetRepository(name)
	if err != nil {
		return nil, err
	}
	return newRepository(repo, name, m.proposer)
}

type pDeleteRepository struct {
	cluster.ProposalBase
	Name string
}

func (m *metadataStore) DeleteRepository(name string) error {
	p := &pDeleteRepository{
		Name: name,
	}
	return m.proposer.Propose(p)
}

func (m *metadataStore) deleteRepository(p cluster.Proposal) cluster.Response {
	v := p.(*pDeleteRepository)
	return m.Metadata.DeleteRepository(v.Name)
}

func newRepository(repo store.Repository, name string, proposer cluster.Proposer) (store.Repository, error) {
	return &repositoryStore{
		Repository: repo,
		name:       name,
		proposer:   proposer,
	}, nil
}

type repositoryStore struct {
	store.Repository
	proposer cluster.Proposer
	name     string
}

type pPutBlobMeta struct {
	cluster.ProposalBase
	Name string
	ID   digest.Digest
}

func (s *repositoryStore) PutBlob(id digest.Digest) error {
	p := &pPutBlobMeta{
		Name: s.name,
		ID:   id,
	}
	return s.proposer.Propose(p)
}

func (m *metadataStore) putBlob(p cluster.Proposal) cluster.Response {
	v := p.(*pPutBlobMeta)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return err
	}
	return repo.PutBlob(v.ID)
}

type pDeleteBlobMeta struct {
	cluster.ProposalBase
	Name string
	ID   digest.Digest
}

func (s *repositoryStore) DeleteBlob(id digest.Digest) error {
	p := &pDeleteBlobMeta{
		Name: s.name,
		ID:   id,
	}
	return s.proposer.Propose(p)
}

func (m *metadataStore) deleteBlob(p cluster.Proposal) cluster.Response {
	v := p.(*pDeleteBlobMeta)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return err
	}
	return repo.DeleteBlob(v.ID)
}

type pPutManifest struct {
	cluster.ProposalBase
	Name string
	ID   digest.Digest
	Meta store.Manifest
	Refs store.References
}

func (s *repositoryStore) PutManifest(id digest.Digest, meta store.Manifest, refs store.References) error {
	p := &pPutManifest{
		Name: s.name,
		ID:   id,
		Meta: meta,
		Refs: refs,
	}
	return s.proposer.Propose(p)
}

func (m *metadataStore) putManifest(p cluster.Proposal) cluster.Response {
	v := p.(*pPutManifest)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return err
	}

	return repo.PutManifest(v.ID, v.Meta, v.Refs)
}

type pDeleteManifest struct {
	cluster.ProposalBase
	Name string
	ID   digest.Digest
}

type rDeleteManifest struct {
	cluster.ResponseBase
	IDs []digest.Digest
}

func (s *repositoryStore) DeleteManifest(id digest.Digest) ([]digest.Digest, error) {
	p := &pDeleteManifest{
		Name: s.name,
		ID:   id,
	}
	r := s.proposer.Propose(p).(*rDeleteManifest)
	return r.IDs, r.Err
}

func (m *metadataStore) deleteManifest(p cluster.Proposal) cluster.Response {
	v := p.(*pDeleteManifest)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return err
	}

	digests, err := repo.DeleteManifest(v.ID)
	r := new(rDeleteManifest)
	r.IDs = digests
	r.Err = err
	return r
}

type pPutTag struct {
	cluster.ProposalBase
	Name   string
	Tag    string
	Digest digest.Digest
}

func (s *repositoryStore) PutTag(tag string, digest digest.Digest) error {
	p := &pPutTag{
		Name:   s.name,
		Tag:    tag,
		Digest: digest,
	}
	return s.proposer.Propose(p)
}

func (m *metadataStore) putTag(p cluster.Proposal) cluster.Response {
	v := p.(*pPutTag)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return err
	}

	return repo.PutTag(v.Tag, v.Digest)
}

type pDeleteTag struct {
	cluster.ProposalBase
	Name string
	Tag  string
}

type rDeleteTag struct {
	cluster.ResponseBase
	IDs []digest.Digest
}

func (s *repositoryStore) DeleteTag(tag string) ([]digest.Digest, error) {
	p := &pDeleteTag{
		Name: s.name,
		Tag:  tag,
	}
	r := s.proposer.Propose(p).(*rDeleteTag)
	return r.IDs, r.Err
}

func (m *metadataStore) deleteTag(p cluster.Proposal) cluster.Response {
	v := p.(*pDeleteTag)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return err
	}

	digests, err := repo.DeleteTag(v.Tag)
	r := new(rDeleteTag)
	r.IDs = digests
	r.Err = err
	return r
}

type pPutUploadSession struct {
	cluster.ProposalBase
	Name    string
	Session *store.UploadSession
}

func (s *repositoryStore) PutUploadSession(session *store.UploadSession) error {
	p := &pPutUploadSession{
		Name:    s.name,
		Session: session,
	}
	return s.proposer.Propose(p)
}

func (m *metadataStore) putUploadSession(p cluster.Proposal) cluster.Response {
	v := p.(*pPutUploadSession)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return err
	}
	return repo.PutUploadSession(v.Session)
}

type pDeleteUploadSession struct {
	cluster.ProposalBase
	Name      string
	SessionID uuid.UUID
}

func (s *repositoryStore) DeleteUploadSession(id uuid.UUID) error {
	p := &pDeleteUploadSession{
		Name:      s.name,
		SessionID: id,
	}
	return s.proposer.Propose(p)
}

func (m *metadataStore) deleteUploadSession(p cluster.Proposal) cluster.Response {
	v := p.(*pDeleteUploadSession)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return err
	}
	return repo.DeleteUploadSession(v.SessionID)
}
