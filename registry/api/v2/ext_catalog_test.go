package v2

import (
	"net/http"
	"testing"

	v1 "github.com/opencontainers/distribution-spec/specs-go/v1"
	. "github.com/robinkb/cascade/testing"
	testclient "github.com/robinkb/cascade/testing/client"
	mock "github.com/robinkb/cascade/testing/mock/registry"
)

func TestListRepositories(t *testing.T) {
	names := RandomNames(20)

	t.Run("Listing tags returns 200", func(t *testing.T) {
		svc := mock.NewService(t)
		svc.EXPECT().
			ListRepositories(-1, "").
			Return(names, nil)

		srv := New(svc)
		client := testclient.NewForHandler(t, srv)

		resp := client.ListRepositories(nil)
		AssertResponseCode(t, resp, http.StatusOK)
		var repoList v1.RepositoryList
		AssertResponseBodyUnmarshals(t, resp, &repoList)
		AssertSlicesEqual(t, repoList.Repositories, names)
	})

	t.Run("n and last query parameters are handled correctly", func(t *testing.T) {
		count := 3
		last := names[10]

		svc := mock.NewService(t)
		svc.EXPECT().
			ListRepositories(count, last).
			Return(names[10:13], nil)

		srv := New(svc)
		client := testclient.NewForHandler(t, srv)

		resp := client.ListRepositories(&testclient.ListOptions{
			N:    testclient.Pointer(count),
			Last: last,
		})

		AssertResponseCode(t, resp, http.StatusOK)
		var repoList v1.RepositoryList
		AssertResponseBodyUnmarshals(t, resp, &repoList)
		AssertSlicesEqual(t, repoList.Repositories, names[10:13])
	})
}
