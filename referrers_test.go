package cascade_test

import (
	"testing"

	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestListReferrers(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Invalid digest returns error", func(t *testing.T) {
		repository := RandomName()
		digest := "12345"

		_, err := service.ListReferrers(repository, digest)

		AssertErrorIs(t, err, cascade.ErrDigestInvalid)
	})
}
