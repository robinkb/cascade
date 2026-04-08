package fs

import (
	"os"
	"testing"

	"github.com/robinkb/cascade/registry/store"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
	. "github.com/robinkb/cascade/testing"
	"github.com/stretchr/testify/suite"
)

func TestBlobSuite(t *testing.T) {
	suite.Run(t, &storesuite.BlobSuite{
		Constructor: func(t *testing.T) store.Blobs {
			tmp := t.TempDir()
			t.Cleanup(func() {
				err := os.RemoveAll(tmp)
				AssertNoError(t, err).Require()
			})

			return NewBlobStore(tmp)
		},
	})
}
