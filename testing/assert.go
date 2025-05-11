package testing

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/textproto"
	"reflect"
	"slices"
	"testing"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/robinkb/cascade-registry"
)

func AssertErrorIs(t *testing.T, got, want error) bool {
	t.Helper()

	if !errors.Is(got, want) {
		t.Errorf("unexpected error: got %q, want %q", got, want)
		return false
	}
	return true
}

func AssertNoError(t *testing.T, got error) bool {
	t.Helper()

	if got != nil {
		t.Errorf("unexpected error: %s", got)
		return false
	}
	return true
}

func AssertResponseCode(t *testing.T, got *http.Response, want int) bool {
	t.Helper()

	if got.StatusCode != want {
		t.Errorf("unexpected response code; got %q, want %q",
			httpStatusText(got.StatusCode),
			httpStatusText(want),
		)
		return false
	}
	return true
}

func AssertResponseHeader(t *testing.T, got *http.Response, header string, want ...string) bool {
	t.Helper()

	// Normalize header name, because they are supposed to be case-insensitive.
	// Go's HTTP library also does this when setting a header on a response.
	header = textproto.CanonicalMIMEHeaderKey(header)

	val, ok := got.Header[header]
	if !ok {
		t.Errorf("header %q is not set", header)
		return false
	}

	if !slices.Equal(val, want) {
		t.Errorf("unexpected value for header %q; got %q, want %q", header, val, want)
		return false
	}
	return true
}

func AssertResponseHeaderSet(t *testing.T, got *http.Response, header string) bool {
	t.Helper()

	header = textproto.CanonicalMIMEHeaderKey(header)

	_, ok := got.Header[header]
	if !ok {
		t.Errorf("header %q is not set", header)
		return false
	}
	return true
}

func AssertResponseHeaderUnset(t *testing.T, got *http.Response, header string) bool {
	t.Helper()

	header = textproto.CanonicalMIMEHeaderKey(header)

	_, ok := got.Header[header]
	if ok {
		t.Errorf("header %q is set", header)
		return false
	}
	return true
}

func AssertResponseBodyEquals(t *testing.T, got *http.Response, want []byte) bool {
	t.Helper()

	data, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("unexpected error while reading response body: %s", err)
		return false
	}

	if !bytes.Equal(data, want) {
		t.Errorf("request did not return the expected content")
		return false
	}
	return true
}

func AssertResponseBodyUnmarshals[T any](t *testing.T, got *http.Response, obj T) bool {
	t.Helper()

	data, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("unexpected error while reading response body: %s", err)
		return false
	}

	err = json.Unmarshal(data, obj)
	if err != nil {
		t.Errorf("could not unmarshal response body as %T: %s", obj, err)
		return false
	}
	return true
}

func AssertResponseBodyContainsError(t *testing.T, got *http.Response, want cascade.Error) bool {
	t.Helper()

	if got.Body == nil || got.Body == http.NoBody {
		t.Errorf("response body is empty while expecting error")
		return false
	}

	var errs cascade.ErrorResponse
	err := json.NewDecoder(got.Body).Decode(&errs)
	RequireNoError(t, err)

	for _, err := range errs.Errors {
		if errors.Is(err, want) {
			return true
		}
	}

	t.Errorf("could not find error in response body; got %q, want %q", errs, want)
	return false
}

func AssertIndex(t *testing.T, got, want *v1.Index) bool {
	t.Helper()

	if len(got.Manifests) != len(want.Manifests) {
		t.Errorf("unexpected descriptor count; got %d, want %d", len(got.Manifests), len(want.Manifests))
		return false
	}

	slices.SortStableFunc(got.Manifests, descriptorSortFunc)
	slices.SortStableFunc(want.Manifests, descriptorSortFunc)

	for i := range got.Manifests {
		gotDescriptor := got.Manifests[i]
		wantDescriptor := want.Manifests[i]

		if !AssertStructsEqual(t, gotDescriptor, wantDescriptor) {
			return false
		}
	}

	return true
}

func AssertEqual[T comparable](t *testing.T, got, want T) bool {
	t.Helper()

	if got != want {
		t.Errorf("values are not equal; got %v, want %v", got, want)
		return false
	}

	return true
}

func AssertSlicesEqual[S ~[]E, E comparable](t *testing.T, got S, wamt S) bool {
	t.Helper()

	if len(got) != len(wamt) {
		t.Errorf("slices are of unequal length; got %d, want %d", len(wamt), len(got))
		return false
	}

	if !slices.Equal(got, wamt) {
		t.Errorf("slices are not equal;\ngot\n%v\nwant\n%v", wamt, got)
		return false
	}
	return true
}

func AssertMapsEqual[M1, M2 ~map[K]V, K, V comparable](t *testing.T, got M1, want M2) bool {
	t.Helper()

	if !maps.Equal(got, want) {
		t.Errorf("maps are not equal; got %+v, want %+v", got, want)
		return false
	}

	return true
}

func AssertStructsEqual(t *testing.T, got, want any) bool {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("structs are not equal;\ngot:\n%+v\nwant:\n%+v", got, want)
		return false
	}

	return true
}

func httpStatusText(code int) string {
	return fmt.Sprintf("%d %s", code, http.StatusText(code))
}

func descriptorSortFunc(a, b v1.Descriptor) int {
	if a.Digest.String() < b.Digest.String() {
		return -1
	}
	if a.Digest.String() > b.Digest.String() {
		return 1
	}
	return 0
}
