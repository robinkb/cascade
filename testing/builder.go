package testing

import (
	"encoding/json"
	"testing"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade/registry/store"
)

func NewImageManifestBuilder(t *testing.T) *ImageManifestBuilder {
	return &ImageManifestBuilder{
		t: t,
	}
}

type ImageManifestBuilder struct {
	t *testing.T
	m ImageManifest
}

func (b *ImageManifestBuilder) WithLayers(n int) *ImageManifestBuilder {
	layers := make([]v1.Descriptor, n)
	for i := range n {
		content := RandomBytes(32)
		id := digest.FromBytes(content)
		layers[i] = v1.Descriptor{
			MediaType: v1.MediaTypeImageLayer,
			Digest:    id,
			Size:      int64(len(content)),
		}
	}
	b.m.Manifest.Layers = layers

	return b
}

func (b *ImageManifestBuilder) WithSubject(subject v1.Manifest) *ImageManifestBuilder {
	data, err := json.Marshal(&subject)
	AssertNoError(b.t, err).Require()
	b.m.Manifest.Subject = &v1.Descriptor{
		MediaType: subject.MediaType,
		Digest:    digest.FromBytes(data),
		Size:      int64(len(data)),
	}
	return b
}

func (b *ImageManifestBuilder) Build() ImageManifest {
	b.m.Manifest.MediaType = v1.MediaTypeImageManifest

	data := RandomBytes(32)
	b.m.Manifest.Config = v1.Descriptor{
		MediaType: v1.MediaTypeImageConfig,
		Digest:    digest.FromBytes(data),
		Size:      int64(len(data)),
	}

	data, err := json.Marshal(&b.m.Manifest)
	AssertNoError(b.t, err).Require()
	b.m.Bytes = data
	b.m.Digest = digest.FromBytes(data)
	return b.m
}

type ImageManifest struct {
	// Manifest is the struct representation of the image manifest.
	Manifest v1.Manifest
	// Bytes is the Manifest data marshalled as JSON.
	Bytes []byte
	// Digest is the digest of the Manifest's data marshalled as JSON.
	Digest digest.Digest
	// Layers is the content of the Manifest's layers.
	// The layers are in the same order as the layers in the Manifest.
	Layers [][]byte
}

func (m *ImageManifest) Metadata() store.Manifest {
	return store.Manifest{
		Annotations:  m.Manifest.Annotations,
		ArtifactType: m.Manifest.Config.MediaType,
		MediaType:    m.Manifest.MediaType,
		Size:         int64(len(m.Bytes)),
	}
}

func (m *ImageManifest) References() store.References {
	refs := store.References{
		Config: m.Manifest.Config.Digest,
		Layers: m.LayersAsDigests(),
	}
	if m.Manifest.Subject != nil {
		refs.Subject = m.Manifest.Subject.Digest
	}
	return refs
}

func (m *ImageManifest) LayersAsDigests() []digest.Digest {
	digests := make([]digest.Digest, len(m.Manifest.Layers))
	for i, layer := range m.Manifest.Layers {
		digests[i] = layer.Digest
	}
	return digests
}

func NewImageIndexBuilder(t *testing.T) ImageIndexBuilder {
	return ImageIndexBuilder{
		t: t,
	}
}

type ImageIndexBuilder struct {
	t *testing.T
}

type ImageIndex struct {
	// Index is the struct representation of the generated image index.
	index v1.Index
}
