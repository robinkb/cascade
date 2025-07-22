package server_test

import (
	"net/http"
	"testing"

	"github.com/robinkb/cascade-registry/server"
	. "github.com/robinkb/cascade-registry/testing"
	testclient "github.com/robinkb/cascade-registry/testing/client"
	"github.com/robinkb/cascade-registry/testing/mock"
)

func TestListTags(t *testing.T) {
	name := RandomName()
	tags := RandomTags(20)

	t.Run("Listing tags returns 200", func(t *testing.T) {
		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			ListTags(name, -1, "").
			Return(tags, nil)

		client := testclient.NewTestClientForRepository(t, name, repo)

		resp := client.ListTags(name, nil)

		AssertResponseCode(t, resp, http.StatusOK)
		var tagsList server.TagsListResponse
		AssertResponseBodyUnmarshals(t, resp, &tagsList)
		AssertSlicesEqual(t, tagsList.Tags, tags)
	})

	t.Run("n and last query parameters are handled correctly", func(t *testing.T) {
		count := 3
		last := tags[10]

		repo := mock.NewRepositoryService(t)
		repo.EXPECT().
			ListTags(name, count, last).
			Return(tags[10:13], nil)

		client := testclient.NewTestClientForRepository(t, name, repo)

		resp := client.ListTags(name, &testclient.ListTagsOptions{
			N:    testclient.Pointer(count),
			Last: last,
		})

		AssertResponseCode(t, resp, http.StatusOK)
		var tagsList server.TagsListResponse
		AssertResponseBodyUnmarshals(t, resp, &tagsList)
		AssertSlicesEqual(t, tagsList.Tags, tags[10:13])
	})
}
