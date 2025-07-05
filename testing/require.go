package testing

import "testing"

func RequireNoError(t *testing.T, err error) {
	t.Helper()

	if !AssertNoError(t, err).success {
		t.FailNow()
	}
}
