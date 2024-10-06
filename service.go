package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
)

func NewRegistryService(store RegistryStore) *registryService {
	return &registryService{
		store:        store,
		sessionStore: make(map[string]map[string]bool),
	}
}

type registryService struct {
	store         RegistryStore
	blobStore     map[string]map[string][]byte
	manifestStore map[string]map[string][]byte
	sessionStore  map[string]map[string]bool
}

func (s *registryService) StatBlob(name, digest string) bool {
	path := fmt.Sprintf("blobs/%s/%s", name, digest)
	return s.store.Stat(path)
}

func (s *registryService) GetBlob(name, digest string) io.Reader {
	path := fmt.Sprintf("blobs/%s/%s", name, digest)
	r, _ := s.store.Get(path)
	return r
}

func (s *registryService) WriteBlob(name string, digest digest.Digest, r io.Reader) bool {
	path := fmt.Sprintf("blobs/%s/%s", name, digest.String())

	verifier := digest.Verifier()
	tee := io.TeeReader(r, verifier)
	buf := bytes.NewBuffer([]byte{})
	io.Copy(buf, tee)

	if verifier.Verified() {
		s.store.Put(path, buf)
		return true
	}

	return false
}

func (s *registryService) StatManifest(name, reference string) (bool, int) {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	// TODO: Stat should not read the data.
	buf := bytes.NewBuffer([]byte{})
	r, err := s.store.Get(path)
	if err != nil {
		return false, 0
	}

	io.Copy(buf, r)
	size := buf.Len()
	return true, size
}

func (s *registryService) GetManifest(name, reference string) []byte {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	// TODO: Stat should not read the data.
	buf := bytes.NewBuffer([]byte{})
	r, err := s.store.Get(path)
	if err != nil {
		return nil
	}

	io.Copy(buf, r)
	return buf.Bytes()
}

func (s *registryService) PutManifest(name, reference string, data []byte) {
	path := fmt.Sprintf("manifests/%s/%s", name, reference)

	s.store.Put(path, bytes.NewBuffer(data))
}

func (s *registryService) InitUploadSession(name string) *UploadSession {
	id, _ := uuid.NewV7()
	if _, ok := s.sessionStore[name]; !ok {
		s.sessionStore[name] = make(map[string]bool)
	}
	sid := id.String()
	s.sessionStore[name][sid] = true
	return &UploadSession{
		ID:       sid,
		Location: fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, sid),
	}
}

func (s *registryService) ActiveUploadSession(name, id string) bool {
	if _, ok := s.sessionStore[name]; ok {
		return s.sessionStore[name][id]
	}
	return false
}
