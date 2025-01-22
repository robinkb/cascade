package testing

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/moby/moby/pkg/namesgenerator"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
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

func RandomManifest() (name string, id digest.Digest, manifest *cascade.Manifest) {
	name = RandomName()
	content, _ := json.Marshal(v1.Manifest{
		MediaType: v1.MediaTypeImageManifest,
	})
	id = digest.FromBytes(content)
	manifest, _ = cascade.NewManifest(content)
	return
}

func RandomBlob(length int64) (name string, id digest.Digest, content []byte) {
	name = RandomName()
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
