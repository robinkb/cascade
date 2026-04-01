package testing

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade/registry/store"
)

const (
	charset = "0123456789" +
		"abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func RandomName() string {
	b := new(strings.Builder)
	b.WriteString(RandomString(rand.IntN(6) + 6))
	b.WriteRune('/')
	b.WriteString(RandomString(rand.IntN(6) + 6))
	return b.String()
}

func RandomNames(n int) []string {
	names := make([]string, n)
	for i := range n {
		names[i] = RandomName()
	}
	return names
}

func RandomBytes(length int64) []byte {
	data := make([]byte, length)
	// nolint: errcheck
	crand.Read(data)
	return data
}

func RandomBytesN(n int, minSize, maxSize int64) [][]byte {
	values := make([][]byte, n)
	for i := range values {
		values[i] = RandomBytes(rand.Int64N(maxSize-minSize) + minSize)
	}
	return values
}

func RandomString(length int) string {
	data := make([]byte, length)
	for i := range data {
		data[i] = charset[rand.IntN(len(charset))]
	}
	return string(data)
}

func RandomUUID() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}
	return id
}

func RandomPort() int {
	return rand.IntN(30000) + 1024
}

func RandomDigest() digest.Digest {
	return digest.FromBytes(RandomBytes(32))
}

func RandomManifest() (id digest.Digest, manifest *v1.Manifest, content []byte) {
	manifest = &v1.Manifest{
		MediaType: v1.MediaTypeImageManifest,
		Annotations: map[string]string{
			// Small amount of random content to make sure that
			// every generated manifest has a unique digest.
			"random": RandomString(8),
		},
	}
	content, _ = json.Marshal(manifest)
	id = digest.FromBytes(content)
	return
}

func RandomManifestWithSubject(subjDigest digest.Digest, subject *v1.Manifest) (id digest.Digest, manifest *v1.Manifest, content []byte) {
	manifest = &v1.Manifest{
		MediaType: v1.MediaTypeImageManifest,
		Subject: &v1.Descriptor{
			MediaType: subject.MediaType,
			Digest:    subjDigest,
		},
		Annotations: map[string]string{
			// Small amount of random content to make sure that
			// every generated manifest has a unique digest.
			"random": RandomString(8),
		},
	}
	content, _ = json.Marshal(manifest)
	digest := digest.FromBytes(content)
	return digest, manifest, content
}

func RandomManifestMetadata() store.Manifest {
	return store.Manifest{
		MediaType: v1.MediaTypeImageManifest,
		Annotations: map[string]string{
			// Small amount of random content to make sure that
			// every generated manifest has a unique digest.
			"random": RandomString(8),
		},
		Size: rand.Int64(),
	}
}

func RandomBlob(length int64) (id digest.Digest, content []byte) {
	content = RandomBytes(length)
	id = digest.FromBytes(content)
	return
}

func RandomVersion() string {
	var major, minor, patch int

	major = rand.IntN(5)
	minor = rand.IntN(20)
	patch = rand.IntN(60)

	return fmt.Sprintf("v%d.%d.%d", major, minor, patch)
}

// RandomTags generates a slice filled with random, unique versions.
// The returned slice is unsorted, because some tests require random input
// to ensure that the registry is properly sorting tags internally.
func RandomTags(count int) (tags []string) {
	var version string
	for range count {
		for {
			version = RandomVersion()
			if !slices.Contains(tags, version) {
				tags = append(tags, version)
				break
			}
		}
	}

	return tags
}

// RandomAddPort returns a netip.AddrPort for localhost and a random port.
func RandomAddrPort() netip.AddrPort {
	return netip.MustParseAddrPort(
		fmt.Sprintf("127.0.0.1:%d", RandomPort()),
	)
}

type Referrer struct {
	Digest  digest.Digest
	Content []byte
}

// GenerateReferrersWithIndex generates an index with mostly-static manifests.
// Used for testing the Referrers API. Added manifests should be fine, but take care
// to leave manifest test case 0 as the first manifest, and do not add another manifest
// with the same artifact type.
func GenerateReferrersWithIndex(t *testing.T, subject digest.Digest) (*v1.Index, []*Referrer) {
	t.Helper()

	idx := v1.Index{Manifests: make([]v1.Descriptor, 0)}
	referrers := make([]*Referrer, 0)

	manifest := v1.Manifest{
		Annotations: map[string]string{
			"test.case/number":      "0",
			"test.case/description": "image manifest with an artifact type set",
		},
		MediaType:    v1.MediaTypeImageManifest,
		ArtifactType: "application/vnd.example+type",
		Subject: &v1.Descriptor{
			Digest: subject,
		},
	}

	referrers = appendReferrer(&idx, referrers, manifest, v1.Descriptor{
		ArtifactType: manifest.ArtifactType,
		MediaType:    manifest.MediaType,
		Annotations:  manifest.Annotations,
	})

	manifest = v1.Manifest{
		Annotations: map[string]string{
			"test.case/number":      "1",
			"test.case/description": "image manifest without an artifact type set",
		},
		MediaType: v1.MediaTypeImageManifest,
		Config: v1.Descriptor{
			MediaType: v1.MediaTypeImageConfig,
		},
		Subject: &v1.Descriptor{
			Digest: subject,
		},
	}

	referrers = appendReferrer(&idx, referrers, manifest, v1.Descriptor{
		MediaType:    manifest.MediaType,
		ArtifactType: manifest.Config.MediaType,
		Annotations:  manifest.Annotations,
	})

	manifest = v1.Manifest{
		ArtifactType: "application/vnd.example.index+type",
		Annotations: map[string]string{
			"test.case/number":      "2",
			"test.case/description": "index manifest with an artifact type set",
		},
		MediaType: v1.MediaTypeImageIndex,
		Subject: &v1.Descriptor{
			Digest: subject,
		},
	}

	referrers = appendReferrer(&idx, referrers, manifest, v1.Descriptor{
		MediaType:    manifest.MediaType,
		ArtifactType: manifest.ArtifactType,
		Annotations:  manifest.Annotations,
	})

	manifest = v1.Manifest{
		Annotations: map[string]string{
			"test.case/number":      "3",
			"test.case/description": "index manifest without an artifact type set",
		},
		MediaType: v1.MediaTypeImageIndex,
		Subject: &v1.Descriptor{
			Digest: subject,
		},
	}

	referrers = appendReferrer(&idx, referrers, manifest, v1.Descriptor{
		MediaType:   manifest.MediaType,
		Annotations: manifest.Annotations,
	})

	manifest = v1.Manifest{
		Annotations: map[string]string{
			"test.case/number":      "4",
			"test.case/description": "index manifest without an artifact type set, but with a config mediaType",
		},
		MediaType: v1.MediaTypeImageIndex,
		Config: v1.Descriptor{
			MediaType: v1.MediaTypeImageConfig,
		},
		Subject: &v1.Descriptor{
			Digest: subject,
		},
	}

	referrers = appendReferrer(&idx, referrers, manifest, v1.Descriptor{
		MediaType:   manifest.MediaType,
		Annotations: manifest.Annotations,
	})

	return &idx, referrers
}

func appendReferrer(idx *v1.Index, referrers []*Referrer, manifest v1.Manifest, descriptor v1.Descriptor) []*Referrer {
	content, _ := json.Marshal(&manifest)
	descriptor.Size = int64(len(content))

	digest := digest.FromBytes(content)
	descriptor.Digest = digest

	idx.Manifests = append(idx.Manifests, descriptor)
	referrers = append(referrers, &Referrer{
		Content: content,
		Digest:  digest,
	})

	return referrers
}
