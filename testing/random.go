package testing

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/moby/moby/pkg/namesgenerator"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	charset = "0123456789" +
		"abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func RandomName() string {
	return strings.Replace(namesgenerator.GetRandomName(0), "_", "/", -1)
}

func RandomContents(length int64) []byte {
	data := make([]byte, length)
	crand.Read(data)
	return data
}

func RandomString(length int) string {
	data := make([]byte, length)
	for i := range data {
		data[i] = charset[rand.IntN(len(charset))]
	}
	return string(data)
}

func RandomDigest() digest.Digest {
	return digest.FromBytes(RandomContents(32))
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

func RandomBlob(length int64) (id digest.Digest, content []byte) {
	content = RandomContents(length)
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

func RandomTags(count int) (tags []string) {
	for range count {
		tags = append(tags, RandomVersion())
	}

	slices.Sort(tags)

	return
}

type Referrer struct {
	Digest  digest.Digest
	Content []byte
}

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
