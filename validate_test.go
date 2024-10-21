package main

import "testing"

func TestRepositoryNameValidation(t *testing.T) {
	cases := []struct {
		name  string
		valid bool
	}{
		{"library/ubuntu", true},
		{"ubuntu", true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := validateRepositoryName(c.name)
			want := c.valid

			if got != want {
				t.Errorf("Name validation returned %v; want %v", got, want)
			}
		})
	}
}

func TestReferenceValidation(t *testing.T) {
	cases := []struct {
		name  string
		valid bool
	}{
		{"library/ubuntu", false},
		{"ubuntu", true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := validateTag(c.name)
			want := c.valid

			if got != want {
				t.Errorf("Reference validation returned %v; want %v", got, want)
			}
		})
	}
}
