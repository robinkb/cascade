package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

func TestListTags(t *testing.T) {
	t.Run("Listing tags returns 200", func(t *testing.T) {
		wantName := randomName()
		wantTags := []string{"40", "1.2.3", "latest"}
		server := New(&StubRegistryService{listTags: func(repository string) ([]string, error) {
			if repository == wantName {
				return wantTags, nil
			}
			panic(errDataNotPassedCorrectly)
		}})

		request, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/tags/list", wantName), nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)

		var tagsList TagsListResponse
		err := json.Unmarshal(response.Body.Bytes(), &tagsList)
		assertNoError(t, err)

		gotName := tagsList.Name
		gotTags := tagsList.Tags

		if gotName != wantName {
			t.Errorf("retrieved repository name incorrect; got %q, want %q", gotName, wantName)
		}

		if !slices.Equal(gotTags, wantTags) {
			t.Errorf("retrieved tags incorrect; got %q, want %q", gotTags, wantTags)
		}
	})
}
