package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestListTags(t *testing.T) {
	t.Run("Listing tags returns 200", func(t *testing.T) {
		wantName := RandomName()
		wantTags := []string{"40", "1.2.3", "latest"}
		server := New(&StubRegistryService{listTags: func(repository string, count int, last string) ([]string, error) {
			if repository == wantName && count == -1 && last == "" {
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
		AssertNoError(t, err)

		gotName := tagsList.Name
		gotTags := tagsList.Tags

		if gotName != wantName {
			t.Errorf("retrieved repository name incorrect; got %q, want %q", gotName, wantName)
		}

		if !slices.Equal(gotTags, wantTags) {
			t.Errorf("retrieved tags incorrect; got %q, want %q", gotTags, wantTags)
		}
	})

	t.Run("n and last query parameters are handled correctly", func(t *testing.T) {
		wantName := RandomName()
		wantCount := 3
		wantLast := "40"

		server := New(&StubRegistryService{listTags: func(repository string, count int, last string) ([]string, error) {
			if !(repository == wantName && count == wantCount && last == wantLast) {
				t.Fatal(errDataNotPassedCorrectly)
			}
			return nil, nil
		}})

		request, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("/v2/%s/tags/list", wantName), nil)
		query := request.URL.Query()
		query.Add("n", strconv.Itoa(wantCount))
		query.Add("last", wantLast)
		request.URL.RawQuery = query.Encode()

		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
	})
}
