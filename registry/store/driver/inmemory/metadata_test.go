package inmemory

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/robinkb/cascade/registry/store"
	storesuite "github.com/robinkb/cascade/registry/store/suite"
)

func TestMetadataSuite(t *testing.T) {
	suite.Run(t, &storesuite.MetadataSuite{
		Constructor: func(_ *testing.T) store.Metadata {
			return NewMetadataStore()
		},
	})
}
