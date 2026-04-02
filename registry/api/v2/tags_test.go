package v2

import (
	"net/http"
	"testing"

	v1 "github.com/opencontainers/distribution-spec/specs-go/v1"
	. "github.com/robinkb/cascade/testing"
	testclient "github.com/robinkb/cascade/testing/client"
	mock "github.com/robinkb/cascade/testing/mock/repository"
)

func TestListTags(t *testing.T) {
	name := RandomName()
	tags := RandomTags(20)

	t.Run("Listing tags returns 200", func(t *testing.T) {
		repo := mock.NewService(t)
		repo.EXPECT().
			ListTags(-1, "").
			Return(tags, nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.ListTags(name, nil)

		AssertResponseCode(t, resp, http.StatusOK)
		var tagList v1.TagList
		AssertResponseBodyUnmarshals(t, resp, &tagList)
		AssertSlicesEqual(t, tagList.Tags, tags)
	})

	t.Run("n and last query parameters are handled correctly", func(t *testing.T) {
		count := 3
		last := tags[10]

		repo := mock.NewService(t)
		repo.EXPECT().
			ListTags(count, last).
			Return(tags[10:13], nil)

		client := NewTestClientForRepository(t, name, repo)

		resp := client.ListTags(name, &testclient.ListOptions{
			N:    testclient.Pointer(count),
			Last: last,
		})

		AssertResponseCode(t, resp, http.StatusOK)
		var tagList v1.TagList
		AssertResponseBodyUnmarshals(t, resp, &tagList)
		AssertSlicesEqual(t, tagList.Tags, tags[10:13])
	})
}
