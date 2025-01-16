package cascade

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/opencontainers/go-digest"
	. "github.com/robinkb/cascade-registry/testing"
)

func newTestRegistry() (RegistryService, MetadataStore, BlobStore) {
	metadata := NewInMemoryMetadataStore()
	blobs := NewInMemoryBlobStore()
	service := NewRegistryService(metadata, blobs)

	return service, metadata, blobs
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("got error where none was expected: %v", err)
		t.FailNow()
	}
}

func assertErrorIs(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Errorf("unexpected error: got %q, want %q", got, want)
	}
}

func assertContent(t *testing.T, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Errorf("expected contents to be equal")
	}
}

func randomBlob(length int64) (name string, id digest.Digest, content []byte) {
	name = RandomName()
	content = randomContents(length)
	id = digest.FromBytes(content)
	return
}

func randomName() string {
	return RandomName()
}

func randomContents(length int64) []byte {
	data := make([]byte, length)
	rand.Read(data)
	return data
}
