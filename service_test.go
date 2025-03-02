package cascade_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/moby/moby/pkg/namesgenerator"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/store/inmemory"
)

func newTestRegistry() (cascade.RegistryService, cascade.MetadataStore, cascade.BlobStore) {
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()
	service := cascade.NewRegistryService(metadata, blobs)

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
	name = randomName()
	content = randomContents(length)
	id = digest.FromBytes(content)
	return
}

func randomName() string {
	return strings.Replace(namesgenerator.GetRandomName(0), "_", "/", -1)
}

func randomContents(length int64) []byte {
	data := make([]byte, length)
	rand.Read(data)
	return data
}
