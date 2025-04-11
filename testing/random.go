package testing

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/moby/moby/pkg/namesgenerator"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func RandomName() string {
	return strings.Replace(namesgenerator.GetRandomName(0), "_", "/", -1)
}

func RandomContents(length int64) []byte {
	data := make([]byte, length)
	crand.Read(data)
	return data
}

func RandomDigest() digest.Digest {
	return digest.FromBytes(RandomContents(32))
}

func RandomManifest() (id digest.Digest, manifest *v1.Manifest, content []byte) {
	manifest = &v1.Manifest{
		MediaType: v1.MediaTypeImageManifest,
	}
	content, _ = json.Marshal(manifest)
	id = digest.FromBytes(content)
	return
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
