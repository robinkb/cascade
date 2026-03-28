package boltdb

import (
	"testing"

	"github.com/robinkb/cascade/registry/store"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
	"github.com/stretchr/testify/suite"
)

func TestMetadataSuite(t *testing.T) {
	suite.Run(t, &storesuite.MetadataSuite{
		Constructor: func(t *testing.T) store.Metadata {
			tmp := t.TempDir()
			meta, err := NewMetadataStore(tmp)
			AssertNoError(t, err).Require()
			return meta
		},

		SkipManifest:        true,
		SkipListManifests:   true,
		SkipReferrers:       true,
		SkipTags:            true,
		SkipListTags:        true,
		SkipUploadSessions:  true,
		SkipRecursiveGC:     true,
		SkipSnapshotRestore: true,
	})
}
