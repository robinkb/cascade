package cascade

import "testing"

func TestListReferrers(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Invalid digest returns error", func(t *testing.T) {
		repository := randomName()
		digest := "12345"

		_, err := service.ListReferrers(repository, digest)

		assertErrorIs(t, err, ErrDigestInvalid)
	})
}
