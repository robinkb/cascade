package server

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

var (
	errDataNotPassedCorrectly = errors.New("data not passed correctly")
)

type StubRegistryService struct {
	statBlob   func(repository, digest string) (*cascade.FileInfo, error)
	getBlob    func(repository, digest string) (io.Reader, error)
	deleteBlob func(repository, digest string) error

	statManifest   func(repository, reference string) (*cascade.FileInfo, error)
	getManifest    func(repository, reference string) (*cascade.Manifest, error)
	putManifest    func(repository, reference string, content []byte) error
	deleteManifest func(repository, reference string) error

	listTags  func(repository string, count int, last string) ([]string, error)
	getTag    func(repository, tag string) (string, error)
	putTag    func(repository, tag, digest string) error
	deleteTag func(repository, tag string) error

	listReferrers func(repository string, digest string) (*v1.Index, error)

	initUpload   func(repository string) *cascade.UploadSession
	statUpload   func(repository, sessionID string) (*cascade.FileInfo, error)
	appendUpload func(repository, sessionID string, r io.Reader, offset int64) error
	closeUpload  func(repository, id, digest string) error
}

func (s *StubRegistryService) StatBlob(repository, digest string) (*cascade.FileInfo, error) {
	return s.statBlob(repository, digest)
}
func (s *StubRegistryService) GetBlob(repository, digest string) (io.Reader, error) {
	return s.getBlob(repository, digest)
}
func (s *StubRegistryService) DeleteBlob(repository, digest string) error {
	return s.deleteBlob(repository, digest)
}
func (s *StubRegistryService) StatManifest(repository, reference string) (*cascade.FileInfo, error) {
	return s.statManifest(repository, reference)
}
func (s *StubRegistryService) GetManifest(repository, reference string) (*cascade.Manifest, error) {
	return s.getManifest(repository, reference)
}
func (s *StubRegistryService) PutManifest(repository, reference string, content []byte) error {
	return s.putManifest(repository, reference, content)
}
func (s *StubRegistryService) DeleteManifest(repository, reference string) error {
	return s.deleteManifest(repository, reference)
}
func (s *StubRegistryService) ListTags(repository string, count int, last string) ([]string, error) {
	return s.listTags(repository, count, last)
}
func (s *StubRegistryService) GetTag(repository, tag string) (string, error) {
	return s.getTag(repository, tag)
}
func (s *StubRegistryService) PutTag(repository, tag, digest string) error {
	return s.putTag(repository, tag, digest)
}
func (s *StubRegistryService) DeleteTag(repository, tag string) error {
	return s.deleteTag(repository, tag)
}
func (s *StubRegistryService) InitUpload(repository string) *cascade.UploadSession {
	return s.initUpload(repository)
}
func (s *StubRegistryService) StatUpload(repository, sessionID string) (*cascade.FileInfo, error) {
	return s.statUpload(repository, sessionID)
}
func (s *StubRegistryService) AppendUpload(repository, sessionID string, r io.Reader, offset int64) error {
	return s.appendUpload(repository, sessionID, r, offset)
}
func (s *StubRegistryService) CloseUpload(repository, id, digest string) error {
	return s.closeUpload(repository, id, digest)
}
func (s *StubRegistryService) ListReferrers(repository, reference string) (*v1.Index, error) {
	return s.listReferrers(repository, reference)
}

func TestRoot(t *testing.T) {
	server := newTestServer()

	t.Run("GET /v2/ should return 200", func(t *testing.T) {
		request, _ := http.NewRequest(http.MethodGet, "/v2/", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		AssertResponseCode(t, response.Result(), http.StatusOK)
	})
}

func newTestServer() *Server {
	return New(
		cascade.NewRegistryService(
			inmemory.NewMetadataStore(),
			inmemory.NewBlobStore(),
		),
	)
}
