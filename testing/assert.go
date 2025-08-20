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
)

type Result struct {
	T       testing.TB
	Success bool
}

func (r *Result) Require() {
	if !r.Success {
		r.T.FailNow()
	}
}

func AssertErrorIs(t testing.TB, got, want error) *Result {
	t.Helper()

	if !errors.Is(got, want) {
		t.Errorf("unexpected error: got %q, want %q", got, want)
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertPanics(t testing.TB, want error) {
	got := recover()
	if got == nil {
		t.Error("expected a panic")
	}

	AssertErrorIs(t, got.(error), want)
}

func AssertNoError(t testing.TB, got error) *Result {
	t.Helper()

	if got != nil {
		t.Errorf("unexpected error: %s", got)
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertResponseCode(t testing.TB, got *http.Response, want int) *Result {
	t.Helper()

	if got.StatusCode != want {
		t.Errorf("unexpected response code; got %q, want %q",
			httpStatusText(got.StatusCode),
			httpStatusText(want),
		)
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertResponseHeader(t testing.TB, got *http.Response, header string, want ...string) *Result {
	t.Helper()

	// Normalize header name, because they are supposed to be case-insensitive.
	// Go's HTTP library also does this when setting a header on a response.
	header = textproto.CanonicalMIMEHeaderKey(header)

	val, ok := got.Header[header]
	if !ok {
		t.Errorf("header %q is not set", header)
		return &Result{t, false}
	}

	if !slices.Equal(val, want) {
		t.Errorf("unexpected value for header %q; got %q, want %q", header, val, want)
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertResponseHeaderSet(t testing.TB, got *http.Response, header string) *Result {
	t.Helper()

	header = textproto.CanonicalMIMEHeaderKey(header)

	_, ok := got.Header[header]
	if !ok {
		t.Errorf("header %q is not set", header)
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertResponseHeaderUnset(t testing.TB, got *http.Response, header string) *Result {
	t.Helper()

	header = textproto.CanonicalMIMEHeaderKey(header)

	_, ok := got.Header[header]
	if ok {
		t.Errorf("header %q is set", header)
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertResponseBodyEquals(t testing.TB, got *http.Response, want []byte) *Result {
	t.Helper()

	data, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("unexpected error while reading response body: %s", err)
		return &Result{t, false}
	}

	if !bytes.Equal(data, want) {
		t.Errorf("request did not return the expected content")
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertResponseBodyUnmarshals[T any](t testing.TB, got *http.Response, obj T) *Result {
	t.Helper()

	data, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("unexpected error while reading response body: %s", err)
		return &Result{t, false}
	}

	err = json.Unmarshal(data, obj)
	if err != nil {
		t.Errorf("could not unmarshal response body as %T: %s", obj, err)
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertIndex(t testing.TB, got, want *v1.Index) *Result {
	t.Helper()

	if len(got.Manifests) != len(want.Manifests) {
		t.Errorf("unexpected descriptor count; got %d, want %d", len(got.Manifests), len(want.Manifests))
		return &Result{t, false}
	}

	slices.SortStableFunc(got.Manifests, descriptorSortFunc)
	slices.SortStableFunc(want.Manifests, descriptorSortFunc)

	for i := range got.Manifests {
		gotDescriptor := got.Manifests[i]
		wantDescriptor := want.Manifests[i]

		if !AssertDeepEqual(t, gotDescriptor, wantDescriptor).Success {
			return &Result{t, false}
		}
	}

	return &Result{t, true}
}

func AssertEqual[T comparable](t testing.TB, got, want T) *Result {
	t.Helper()

	if got != want {
		t.Errorf("values are not equal; got %v, want %v", got, want)
		return &Result{t, false}
	}

	return &Result{t, true}
}

func AssertSlicesEqual[S ~[]E, E comparable](t testing.TB, got S, want S) *Result {
	t.Helper()

	if len(got) != len(want) {
		t.Errorf("slices are of unequal length; got %d, want %d", len(got), len(want))
		return &Result{t, false}
	}

	if !slices.Equal(got, want) {
		t.Errorf("slices are not equal")
		if len(want) <= 32 && len(got) <= 32 {
			t.Errorf("got\n%v\nwant\n%v", got, want)
		}
		return &Result{t, false}
	}
	return &Result{t, true}
}

func AssertMapsEqual[M1, M2 ~map[K]V, K, V comparable](t testing.TB, got M1, want M2) *Result {
	t.Helper()

	if !maps.Equal(got, want) {
		t.Errorf("maps are not equal; got %+v, want %+v", got, want)
		return &Result{t, false}
	}

	return &Result{t, true}
}

func AssertDeepEqual(t testing.TB, got, want any) *Result {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("structs are not equal;\ngot:\n%+v\nwant:\n%+v", got, want)
		return &Result{t, false}
	}

	return &Result{t, true}
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
