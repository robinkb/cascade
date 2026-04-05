package cluster

import (
	"encoding/json"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/registry/store"
)

const (
	opCreateRepository    = "M_CREATE_REPOSITORY"
	opDeleteRepository    = "M_DELETE_REPOSITORY"
	opPutBlobMeta         = "M_PUT_BLOB"
	opDeleteBlobMeta      = "M_DELETE_BLOB"
	opPutManifest         = "M_PUT_MANIFEST"
	opDeleteManifest      = "M_DELETE_MANIFEST"
	opPutTag              = "M_PUT_TAG"
	opDeleteTag           = "M_DELETE_TAG"
	opPutUploadSession    = "M_PUT_UPLOAD"
	opDeleteUploadSession = "M_DELETE_UPLOAD"
)

func NewMetadataStore(proposer cluster.Proposer, meta store.Metadata) store.Metadata {
	s := &metadataStore{
		Metadata: meta,
		proposer: proposer,
	}

	proposer.Handle(opCreateRepository, s.createRepository)
	proposer.Handle(opDeleteRepository, s.deleteRepository)
	proposer.Handle(opPutBlobMeta, s.putBlob)
	proposer.Handle(opDeleteBlobMeta, s.deleteBlob)
	proposer.Handle(opPutManifest, s.putManifest)
	proposer.Handle(opDeleteManifest, s.deleteManifest)
	proposer.Handle(opPutTag, s.putTag)
	proposer.Handle(opDeleteTag, s.deleteTag)
	proposer.Handle(opPutUploadSession, s.putUploadSession)
	proposer.Handle(opDeleteUploadSession, s.deleteUploadSession)

	return s
}

type metadataStore struct {
	store.Metadata
	proposer cluster.Proposer
}

func (m *metadataStore) CreateRepository(name string) (store.Repository, error) {
	resp := m.proposer.Propose(cluster.Request{
		Op:   opCreateRepository,
		Data: []byte(name),
	})
	if resp.Err != nil {
		return nil, resp.Err
	}

	return m.GetRepository(name)
}

func (m *metadataStore) createRepository(req cluster.Request) cluster.Response {
	name := string(req.Data)

	_, err := m.Metadata.CreateRepository(name)
	return cluster.Response{
		Err: err,
	}
}

func (m *metadataStore) GetRepository(name string) (store.Repository, error) {
	repo, err := m.Metadata.GetRepository(name)
	if err != nil {
		return nil, err
	}

	return newRepository(repo, name, m.proposer)
}

func (m *metadataStore) DeleteRepository(name string) error {
	resp := m.proposer.Propose(cluster.Request{
		Op:   opDeleteRepository,
		Data: []byte(name),
	})
	return resp.Err
}

func (m *metadataStore) deleteRepository(req cluster.Request) cluster.Response {
	name := string(req.Data)

	err := m.Metadata.DeleteRepository(name)
	return cluster.Response{
		Err: err,
	}
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

type putBlobRequestParams struct {
	Name string
	ID   digest.Digest
}

func (r *repositoryStore) PutBlob(id digest.Digest) error {
	req := &putBlobRequestParams{
		Name: r.name,
		ID:   id,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := r.proposer.Propose(cluster.Request{
		Op:   opPutBlobMeta,
		Data: data,
	})

	return resp.Err
}

func (m *metadataStore) putBlob(req cluster.Request) (resp cluster.Response) {
	var p putBlobRequestParams
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		resp.Err = err
		return
	}

	repo, err := m.Metadata.GetRepository(p.Name)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = repo.PutBlob(p.ID)
	return
}

type deleteBlobMetaParams struct {
	Name string
	ID   digest.Digest
}

func (r *repositoryStore) DeleteBlob(id digest.Digest) error {
	req := &deleteBlobMetaParams{
		Name: r.name,
		ID:   id,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := r.proposer.Propose(cluster.Request{
		Op:   opDeleteBlobMeta,
		Data: data,
	})

	return resp.Err
}

func (m *metadataStore) deleteBlob(req cluster.Request) (resp cluster.Response) {
	var p deleteBlobMetaParams
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		resp.Err = err
		return
	}

	repo, err := m.Metadata.GetRepository(p.Name)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = repo.DeleteBlob(p.ID)
	return
}

type putManifestMetaParams struct {
	Name string
	ID   digest.Digest
	Meta store.Manifest
	Refs store.References
}

func (r *repositoryStore) PutManifest(id digest.Digest, meta store.Manifest, refs store.References) error {
	req := &putManifestMetaParams{
		Name: r.name,
		ID:   id,
		Meta: meta,
		Refs: refs,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := r.proposer.Propose(cluster.Request{
		Op:   opPutManifest,
		Data: data,
	})

	return resp.Err
}

func (m *metadataStore) putManifest(req cluster.Request) (resp cluster.Response) {
	var p putManifestMetaParams
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		resp.Err = err
		return
	}

	repo, err := m.Metadata.GetRepository(p.Name)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = repo.PutManifest(p.ID, p.Meta, p.Refs)
	return
}

type deleteManifestMetaParams struct {
	Name string
	ID   digest.Digest
}

func (r *repositoryStore) DeleteManifest(id digest.Digest) ([]digest.Digest, error) {
	p := &deleteManifestMetaParams{
		Name: r.name,
		ID:   id,
	}
	data, err := json.Marshal(&p)
	if err != nil {
		return nil, err
	}

	resp := r.proposer.Propose(cluster.Request{
		Op:   opDeleteManifest,
		Data: data,
	})
	if resp.Err != nil {
		return nil, resp.Err
	}

	digests := make([]digest.Digest, 0)
	err = json.Unmarshal(resp.Data, &digests)
	return digests, err
}

func (m *metadataStore) deleteManifest(req cluster.Request) (resp cluster.Response) {
	var p deleteManifestMetaParams
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		resp.Err = err
		return
	}

	repo, err := m.Metadata.GetRepository(p.Name)
	if err != nil {
		resp.Err = err
		return
	}

	digests, err := repo.DeleteManifest(p.ID)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Data, resp.Err = json.Marshal(digests)
	return
}

type putTagParams struct {
	Name   string
	Tag    string
	Digest digest.Digest
}

func (r *repositoryStore) PutTag(tag string, digest digest.Digest) error {
	req := &putTagParams{
		Name:   r.name,
		Tag:    tag,
		Digest: digest,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := r.proposer.Propose(cluster.Request{
		Op:   opPutTag,
		Data: data,
	})

	return resp.Err
}

func (m *metadataStore) putTag(req cluster.Request) (resp cluster.Response) {
	var p putTagParams
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		resp.Err = err
		return
	}

	repo, err := m.Metadata.GetRepository(p.Name)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = repo.PutTag(p.Tag, p.Digest)
	return
}

type deleteTagParams struct {
	Name string
	Tag  string
}

func (r *repositoryStore) DeleteTag(tag string) ([]digest.Digest, error) {
	p := &deleteTagParams{
		Name: r.name,
		Tag:  tag,
	}
	data, err := json.Marshal(&p)
	if err != nil {
		return nil, err
	}

	resp := r.proposer.Propose(cluster.Request{
		Op:   opDeleteTag,
		Data: data,
	})
	if resp.Err != nil {
		return nil, resp.Err
	}

	digests := make([]digest.Digest, 0)
	err = json.Unmarshal(resp.Data, &digests)
	return digests, err
}

func (m *metadataStore) deleteTag(req cluster.Request) (resp cluster.Response) {
	var p deleteTagParams
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		resp.Err = err
		return
	}

	repo, err := m.Metadata.GetRepository(p.Name)
	if err != nil {
		resp.Err = err
		return
	}

	digests, err := repo.DeleteTag(p.Tag)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Data, resp.Err = json.Marshal(digests)
	return
}

type putUploadSessionParams struct {
	Name    string
	Session *store.UploadSession
}

func (r *repositoryStore) PutUploadSession(session *store.UploadSession) error {
	req := &putUploadSessionParams{
		Name:    r.name,
		Session: session,
	}
	data, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	resp := r.proposer.Propose(cluster.Request{
		Op:   opPutUploadSession,
		Data: data,
	})

	return resp.Err
}

func (m *metadataStore) putUploadSession(req cluster.Request) (resp cluster.Response) {
	var p putUploadSessionParams
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		resp.Err = err
		return
	}

	repo, err := m.Metadata.GetRepository(p.Name)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = repo.PutUploadSession(p.Session)
	return
}

type deleteUploadSessionParams struct {
	Name      string
	SessionID uuid.UUID
}

func (r *repositoryStore) DeleteUploadSession(id uuid.UUID) error {
	p := &deleteUploadSessionParams{
		Name:      r.name,
		SessionID: id,
	}
	data, err := json.Marshal(&p)
	if err != nil {
		return err
	}

	resp := r.proposer.Propose(cluster.Request{
		Op:   opDeleteUploadSession,
		Data: data,
	})

	return resp.Err
}

func (m *metadataStore) deleteUploadSession(req cluster.Request) (resp cluster.Response) {
	var p deleteUploadSessionParams
	err := json.Unmarshal(req.Data, &p)
	if err != nil {
		resp.Err = err
		return
	}

	repo, err := m.Metadata.GetRepository(p.Name)
	if err != nil {
		resp.Err = err
		return
	}

	resp.Err = repo.DeleteUploadSession(p.SessionID)
	return
}
