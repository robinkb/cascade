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

	idx := v1.Index{
		Manifests: make([]v1.Descriptor, 0),
	}

	referrers := make([]*Referrer, 0)

	imageManifest := v1.Manifest{
		Annotations: map[string]string{
			"test.case/number":      "0",
			"test.case/description": "image manifest with an artifactType set",
		},
		MediaType:    v1.MediaTypeImageManifest,
		ArtifactType: "application/vnd.example+type",
		Subject: &v1.Descriptor{
			Digest: subject,
		},
	}

	imageManifestContent, err := json.Marshal(&imageManifest)
	RequireNoError(t, err)

	imageManifestDigest := digest.FromBytes(imageManifestContent)

	referrers = append(referrers, &Referrer{
		Digest:  imageManifestDigest,
		Content: imageManifestContent,
	})

	idx.Manifests = append(idx.Manifests, v1.Descriptor{
		ArtifactType: imageManifest.ArtifactType,
		MediaType:    imageManifest.MediaType,
		Annotations:  imageManifest.Annotations,
		Digest:       imageManifestDigest,
		Size:         int64(len(imageManifestContent)),
	})

	return &idx, referrers
}
