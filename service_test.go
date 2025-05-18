package cascade_test

import (
	"testing"

	"github.com/robinkb/cascade-registry"
	"github.com/robinkb/cascade-registry/store/inmemory"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestGetRepository(t *testing.T) {
	service := cascade.NewRegistryService(inmemory.NewMetadataStore(), inmemory.NewBlobStore())
	name := RandomName()
	t.Run("Retrieve a repository", func(t *testing.T) {
		_, err := service.GetRepository(name)
		AssertNoError(t, err)
	})
}
