package fs

import (
	"slices"
	"testing"

	"github.com/opencontainers/go-digest"
	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
)

func TestAllBlobs(t *testing.T) {
	count := 10
	dir := t.TempDir()
	blobs := NewBlobStore(dir)

	want := make([]digest.Digest, 0)
	for range count {
		id, content := RandomBlob(32)
		err := blobs.PutBlob(id, content)
		AssertNoError(t, err).Require()
		want = append(want, id)
	}

	got := make([]digest.Digest, 0)
	for id, err := range blobs.AllBlobs() {
		AssertNoError(t, err)
		got = append(got, id)
	}

	slices.Sort(want)
	slices.Sort(got)

	AssertSlicesEqual(t, got, want)
	AssertEqual(t, len(got), count)
}
