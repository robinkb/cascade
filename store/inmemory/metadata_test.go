// Tests in this file should be moved into the store package,
// so that they can be easily run against every metadata store implementation.
package inmemory

import (
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestManifestMetadataPersistence(t *testing.T) {
	metadata := NewMetadataStore()

	name := RandomName()
	digest := RandomDigest()

	wantMetadata := &cascade.ManifestMetadata{
		Annotations: map[string]string{
			"random": RandomString(8),
		},
		MediaType: v1.MediaTypeDescriptor,
		Path:      RandomString(8),
		Size:      42,
	}

	err := metadata.PutManifest(name, digest, wantMetadata)
	RequireNoError(t, err)

	gotMetadata, err := metadata.GetManifest(name, digest)

	AssertStructsEqual(t, gotMetadata, wantMetadata)
}
