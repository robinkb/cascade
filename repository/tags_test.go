package repository_test

import (
	"slices"
	"testing"

	"github.com/robinkb/cascade-registry/repository"
	. "github.com/robinkb/cascade-registry/testing"
)

func (s *Suite) TestGetTag() {
	if s.Tests.TagsDisabled {
		s.T().SkipNow()
	}

	name := s.RandomRepository()

	s.T().Run("Manifest digest is retrievable by tag", func(t *testing.T) {
		digest := RandomDigest()
		tag := "v1.2.3"

		err := s.metadata.PutTag(name, tag, digest)
		RequireNoError(t, err)

		got, err := s.repository.GetTag(name, tag)
		AssertNoError(t, err)

		if got != digest.String() {
			t.Errorf("wrong digest retrieved; got %s, want %s", got, digest.String())
		}
	})

	s.T().Run("Unknown tag returns ErrManifestUnknown", func(t *testing.T) {
		_, err := s.repository.GetTag("non/existent", "v1.2.3")
		AssertErrorIs(t, err, repository.ErrManifestUnknown)
	})
}

func (s *Suite) TestPutTag() {
	if s.Tests.TagsDisabled {
		s.T().SkipNow()
	}

	name := s.RandomRepository()

	s.T().Run("Tag creates a link to the manifest digest", func(t *testing.T) {
		digest, _, content := RandomManifest()
		tag := "v0.5.1"

		_, err := s.repository.PutManifest(name, digest.String(), content)
		AssertNoError(t, err)

		err = s.repository.PutTag(name, tag, digest.String())
		AssertNoError(t, err)

		gotDigest, err := s.repository.GetTag(name, tag)
		AssertNoError(t, err)

		_, gotManifest, err := s.repository.GetManifest(name, gotDigest)
		AssertNoError(t, err)

		AssertSlicesEqual(t, gotManifest, content)
	})

	s.T().Run("Creating tag on unknown repository returns ErrNameUnknown", func(t *testing.T) {
		name := RandomName()
		digest := RandomDigest()
		tag := RandomVersion()

		err := s.repository.PutTag(name, tag, digest.String())
		AssertErrorIs(t, err, repository.ErrNameUnknown)
	})
}

func (s *Suite) TestDeleteTag() {
	if s.Tests.TagsDisabled {
		s.T().SkipNow()
	}

	name := s.RandomRepository()

	s.T().Run("Deleted tag is not retrievable", func(t *testing.T) {
		digest := RandomDigest()
		tag := "v0.5.1"

		err := s.repository.PutTag(name, tag, digest.String())
		RequireNoError(t, err)

		_, err = s.repository.GetTag(name, tag)
		RequireNoError(t, err)

		err = s.repository.DeleteTag(name, tag)
		AssertNoError(t, err)

		_, err = s.repository.GetTag(name, tag)
		AssertErrorIs(t, err, repository.ErrManifestUnknown)
	})
}

func (s *Suite) TestListTag() {
	if s.Tests.TagsDisabled {
		s.T().SkipNow()
	}

	s.T().Run("Listing tags returns all in lexical order", func(t *testing.T) {
		name := s.RandomRepository()
		digest := RandomDigest()
		tags := []string{
			"v1.0.0",
			"v1.1.0",
			"v1.1.1",
			"v1.1.2",
		}

		for _, tag := range tags {
			err := s.repository.PutTag(name, tag, string(digest))
			RequireNoError(t, err)
		}

		got, err := s.repository.ListTags(name, -1, "")
		AssertNoError(t, err)

		want := tags

		AssertSlicesEqual(t, got, want)
	})

	s.T().Run("Listing tags with a count limit returns fewer tags", func(t *testing.T) {
		name := s.RandomRepository()
		digest := RandomDigest()
		tags := RandomTags(20)
		count := 5

		for _, tag := range tags {
			err := s.repository.PutTag(name, tag, digest.String())
			RequireNoError(t, err)
		}

		slices.Sort(tags)

		got, err := s.repository.ListTags(name, count, "")
		AssertNoError(t, err)

		want := tags[0:count]

		AssertSlicesEqual(t, got, want)
	})

	s.T().Run("Listing tags with a count of 0 must return an empty list", func(t *testing.T) {
		name := s.RandomRepository()
		digest := RandomDigest()
		tags := RandomTags(5)
		count := 0

		for _, tag := range tags {
			err := s.repository.PutTag(name, tag, digest.String())
			RequireNoError(t, err)
		}

		got, err := s.repository.ListTags(name, count, "")
		AssertNoError(t, err)

		AssertEqual(t, len(got), count)
	})

	s.T().Run("Listing tags with a count greater than the number of tags returns all tags", func(t *testing.T) {
		name := s.RandomRepository()
		digest := RandomDigest()
		tags := RandomTags(5)
		count := 10

		for _, tag := range tags {
			err := s.repository.PutTag(name, tag, digest.String())
			RequireNoError(t, err)
		}

		slices.Sort(tags)

		got, err := s.repository.ListTags(name, count, "")
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, tags)
	})

	s.T().Run("Listing tags from a certain tag only returns tags after that tag", func(t *testing.T) {
		name := s.RandomRepository()
		digest := RandomDigest()
		tags := RandomTags(10)
		count := 3
		last := 4

		for _, tag := range tags {
			err := s.repository.PutTag(name, tag, digest.String())
			RequireNoError(t, err)
		}

		slices.Sort(tags)

		got, err := s.repository.ListTags(name, count, tags[last])
		AssertNoError(t, err)

		want := tags[last+1 : last+1+count]

		AssertSlicesEqual(t, got, want)
	})

	s.T().Run("When listing tags from a certain tag, the count parameter may be -1 to return all tags", func(t *testing.T) {
		name := s.RandomRepository()
		digest := RandomDigest()
		tags := RandomTags(10)
		count := -1
		last := 4

		for _, tag := range tags {
			err := s.repository.PutTag(name, tag, digest.String())
			RequireNoError(t, err)
		}

		slices.Sort(tags)

		got, err := s.repository.ListTags(name, count, tags[last])
		AssertNoError(t, err)

		want := tags[last+1:]

		AssertSlicesEqual(t, got, want)
	})
}
