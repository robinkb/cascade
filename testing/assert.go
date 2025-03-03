package testing

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
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

	_, ok := got.Header[header]
	if !ok {
		t.Errorf("header %q is not set", header)
		return false
	}
	return true
}

func AssertResponseHeaderUnset(t *testing.T, got *http.Response, header string) bool {
	t.Helper()

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

func AssertSlicesEqual[S ~[]E, E comparable](t *testing.T, s1 S, s2 S) bool {
	t.Helper()

	if len(s1) != len(s2) {
		t.Errorf("slices are of unequal length; got %d, want %d", len(s2), len(s1))
		return false
	}

	if !slices.Equal(s1, s2) {
		t.Errorf("slices are not equal; got %v, want %v", s2, s1)
		return false
	}
	return true
}

func httpStatusText(code int) string {
	return fmt.Sprintf("%d %s", code, http.StatusText(code))
}
