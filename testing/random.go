package testing

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandomSequence(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func RandomName() string {
	return fmt.Sprintf("%s/%s", RandomSequence(16), RandomSequence(16))
}

func RandomContents(length int64) []byte {
	data := make([]byte, length)
	rand.Read(data)
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
