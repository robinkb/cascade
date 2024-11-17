package testing

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
)

func AssertResponseCode(t *testing.T, got *http.Response, want int) {
	t.Helper()

	if got.StatusCode != want {
		t.Errorf("unexpected response code; got %q, want %q",
			httpStatusText(got.StatusCode),
			httpStatusText(want),
		)
	}
}

func AssertResponseHeader(t *testing.T, got *http.Response, header string, want ...string) {
	t.Helper()

	val, ok := got.Header[header]
	if !ok {
		t.Errorf("header %q is not set", header)
		return
	}

	if !slices.Equal(val, want) {
		t.Errorf("unexpected value for header %q; got %q, want %q", header, val, want)
	}
}

func AssertResponseBody(t *testing.T, got *http.Response, want []byte) {
	t.Helper()

	data, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("unexpected error while reading response body: %s", err)
	}

	if !bytes.Equal(data, want) {
		t.Errorf("request did not return the expected content")
	}
}

func httpStatusText(code int) string {
	return fmt.Sprintf("%d %s", code, http.StatusText(code))
}
