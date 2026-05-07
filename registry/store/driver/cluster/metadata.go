package cluster

import (
	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/registry/store"
)

const (
	tCreateRepository cluster.ProposalType = iota + 100
	tDeleteRepository
	tPutLink
	tDeleteLink
	tPutManifest
	tDeleteManifest
	tPutTag
	tDeleteTag
	tPutUploadSession
	tDeleteUploadSession
)

func NewMetadataStore(proposer cluster.Proposer, meta store.Metadata) store.Metadata {
	s := &metadataStore{
		Metadata: meta,
		proposer: proposer,
	}

	proposer.Handle(tCreateRepository, s.createRepository)
	proposer.Handle(tDeleteRepository, s.deleteRepository)
	proposer.Handle(tPutLink, s.putLink)
	proposer.Handle(tDeleteLink, s.deleteLink)
	proposer.Handle(tPutManifest, s.putManifest)
	proposer.Handle(tDeleteManifest, s.deleteManifest)
	proposer.Handle(tPutTag, s.putTag)
	proposer.Handle(tDeleteTag, s.deleteTag)
	proposer.Handle(tPutUploadSession, s.putUploadSession)
	proposer.Handle(tDeleteUploadSession, s.deleteUploadSession)

	return s
}

type metadataStore struct {
	store.Metadata
	proposer cluster.Proposer
}

type pCreateRepository struct {
	Name string
}

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	p := &pCreateRepository{
		Name: name,
	}
	_, err := m.proposer.Propose(tCreateRepository, mustMarshal(p))
	if err != nil {
		return nil, err
	}

	return m.GetRepository(name)
}

func (m *metadataStore) createRepository(data []byte) (resp any, err error) {
	v := new(pCreateRepository)
	mustUnmarshal(data, v)
	_, err = m.Metadata.CreateRepository(v.Name)
	return
}

func (m *metadataStore) GetRepository(name string) (store.Repository, error) {
	repo, err := m.Metadata.GetRepository(name)
	if err != nil {
		return nil, err
	}
	return newRepository(repo, name, m.proposer)
}

type pDeleteRepository struct {
	Name string
}

func (m *metadataStore) DeleteRepository(name string) error {
	p := &pDeleteRepository{
		Name: name,
	}
	_, err := m.proposer.Propose(tDeleteRepository, mustMarshal(p))
	return err
}

func (m *metadataStore) deleteRepository(data []byte) (resp any, err error) {
	v := new(pDeleteRepository)
	mustUnmarshal(data, v)
	err = m.Metadata.DeleteRepository(v.Name)
	return
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

type pPutLink struct {
	Name string
	ID   digest.Digest
}

func (s *repositoryStore) PutLink(id digest.Digest) error {
	p := &pPutLink{
		Name: s.name,
		ID:   id,
	}
	_, err := s.proposer.Propose(tPutLink, mustMarshal(p))
	return err
}

func (m *metadataStore) putLink(data []byte) (resp any, err error) {
	v := new(pPutLink)
	mustUnmarshal(data, v)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return nil, err
	}
	err = repo.PutLink(v.ID)
	return
}

type pDeleteLink struct {
	Name string
	ID   digest.Digest
}

func (s *repositoryStore) DeleteLink(id digest.Digest) error {
	p := &pDeleteLink{
		Name: s.name,
		ID:   id,
	}
	_, err := s.proposer.Propose(tDeleteLink, mustMarshal(p))
	return err
}

func (m *metadataStore) deleteLink(data []byte) (resp any, err error) {
	v := new(pDeleteLink)
	mustUnmarshal(data, v)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return nil, err
	}
	err = repo.DeleteLink(v.ID)
	return
}

type pPutManifest struct {
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
	_, err := s.proposer.Propose(tPutManifest, mustMarshal(p))
	return err
}

func (m *metadataStore) putManifest(data []byte) (resp any, err error) {
	v := new(pPutManifest)
	mustUnmarshal(data, v)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return nil, err
	}

	err = repo.PutManifest(v.ID, v.Meta, v.Refs)
	return
}

type pDeleteManifest struct {
	Name string
	ID   digest.Digest
}

func (s *repositoryStore) DeleteManifest(id digest.Digest) ([]digest.Digest, error) {
	p := &pDeleteManifest{
		Name: s.name,
		ID:   id,
	}
	resp, err := s.proposer.Propose(tDeleteManifest, mustMarshal(p))
	return resp.([]digest.Digest), err
}

func (m *metadataStore) deleteManifest(data []byte) (resp any, err error) {
	v := new(pDeleteManifest)
	mustUnmarshal(data, v)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return nil, err
	}

	resp, err = repo.DeleteManifest(v.ID)
	return
}

type pPutTag struct {
	Name   string
	Tag    string
	Digest digest.Digest
}

func (s *repositoryStore) PutTag(tag string, id digest.Digest) ([]digest.Digest, error) {
	p := &pPutTag{
		Name:   s.name,
		Tag:    tag,
		Digest: id,
	}
	resp, err := s.proposer.Propose(tPutTag, mustMarshal(p))
	return resp.([]digest.Digest), err
}

func (m *metadataStore) putTag(data []byte) (resp any, err error) {
	v := new(pPutTag)
	mustUnmarshal(data, v)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return nil, err
	}

	resp, err = repo.PutTag(v.Tag, v.Digest)
	return
}

type pDeleteTag struct {
	Name string
	Tag  string
}

func (s *repositoryStore) DeleteTag(tag string) ([]digest.Digest, error) {
	p := &pDeleteTag{
		Name: s.name,
		Tag:  tag,
	}
	resp, err := s.proposer.Propose(tDeleteTag, mustMarshal(p))
	if err != nil {
		return nil, err
	}
	return resp.([]digest.Digest), nil
}

func (m *metadataStore) deleteTag(data []byte) (resp any, err error) {
	v := new(pDeleteTag)
	mustUnmarshal(data, v)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return nil, err
	}

	resp, err = repo.DeleteTag(v.Tag)
	return
}

func (s *repositoryStore) GetUploadSession(id uuid.UUID) (*store.UploadSession, error) {
	if err := s.proposer.ReadState(); err != nil {
		return nil, err
	}

	return s.Repository.GetUploadSession(id)
}

type pPutUploadSession struct {
	Name    string
	Session *store.UploadSession
}

func (s *repositoryStore) PutUploadSession(session *store.UploadSession) error {
	p := &pPutUploadSession{
		Name:    s.name,
		Session: session,
	}
	_, err := s.proposer.Propose(tPutUploadSession, mustMarshal(p))
	return err
}

func (m *metadataStore) putUploadSession(data []byte) (resp any, err error) {
	v := new(pPutUploadSession)
	mustUnmarshal(data, v)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return nil, err
	}

	err = repo.PutUploadSession(v.Session)
	return
}

type pDeleteUploadSession struct {
	Name      string
	SessionID uuid.UUID
}

func (s *repositoryStore) DeleteUploadSession(id uuid.UUID) error {
	p := &pDeleteUploadSession{
		Name:      s.name,
		SessionID: id,
	}
	_, err := s.proposer.Propose(tDeleteUploadSession, mustMarshal(p))
	return err
}

func (m *metadataStore) deleteUploadSession(data []byte) (resp any, err error) {
	v := new(pDeleteUploadSession)
	mustUnmarshal(data, v)
	repo, err := m.Metadata.GetRepository(v.Name)
	if err != nil {
		return nil, err
	}
	err = repo.DeleteUploadSession(v.SessionID)
	return
}
