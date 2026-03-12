package inmemory

import (
	"testing"

	"github.com/robinkb/cascade-registry/registry/store"
	storesuite "github.com/robinkb/cascade-registry/registry/store/suite"
	"github.com/stretchr/testify/suite"
)

func TestMetadataSuite(t *testing.T) {
	suite.Run(t, &storesuite.MetadataSuite{
		Constructor: func() store.Metadata {
			return NewMetadataStore()
		},
	})
}
