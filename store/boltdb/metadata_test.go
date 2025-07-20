package boltdb

import (
	"os"
	"testing"

	"github.com/robinkb/cascade-registry/store"
	storesuite "github.com/robinkb/cascade-registry/store/suite"
	"github.com/stretchr/testify/suite"
)

func TestMetadataSuite(t *testing.T) {
	suite.Run(t, &storesuite.MetadataSuite{
		Constructor: func() store.Metadata {
			tmp := t.TempDir()
			t.Cleanup(func() {
				os.RemoveAll(tmp) // nolint: errcheck
			})

			return NewMetadataStore(tmp)
		},
	})
}
