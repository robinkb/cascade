// Tests in this file should be moved into the store package,
// so that they can be easily run against every metadata store implementation.
package inmemory

import (
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry/store"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestManifestMetadataPersistence(t *testing.T) {
	metadata := NewMetadataStore()

	name := RandomName()
	digest := RandomDigest()

	wantMetadata := &store.ManifestMetadata{
		Annotations: map[string]string{
			"random": RandomString(8),
		},
		MediaType: v1.MediaTypeDescriptor,
		Size:      42,
	}

	err := metadata.PutManifest(name, digest, wantMetadata)
	RequireNoError(t, err)

	gotMetadata, err := metadata.GetManifest(name, digest)
	AssertNoError(t, err)
	AssertStructsEqual(t, gotMetadata, wantMetadata)
}
