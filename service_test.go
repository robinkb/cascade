package main

import (
	"errors"
	"testing"
)

func TestServiceStatBlob(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.StatBlob("a", "b")
		assertErrorIs(t, err, ErrBlobUnknown)
	})
}

func TestServiceGetBlob(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("unknown blob returns ErrBlobUnknown", func(t *testing.T) {
		_, err := service.GetBlob("a", "b")
		assertErrorIs(t, err, ErrBlobUnknown)
	})
}

func assertErrorIs(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Errorf("unexpected error: got %q, want %q", got, want)
	}
}
