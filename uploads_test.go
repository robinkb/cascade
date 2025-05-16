package cascade_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestStatUpload(t *testing.T) {
	service, _, _ := newTestRepository()

	t.Run("stat upload returns correct FileInfo", func(t *testing.T) {
		repository := "a/v/c"
		content := RandomContents(32)

		session, err := service.InitUpload(repository)
		RequireNoError(t, err)
		err = service.AppendUpload(repository, session.ID.String(), bytes.NewBuffer(content), 0)
		RequireNoError(t, err)

		info, err := service.StatUpload(repository, session.ID.String())
		AssertNoError(t, err)

		got := info.Size
		want := len(content)

		if info.Size != int64(len(content)) {
			t.Errorf("got unexpected upload size %d, want %d", got, want)
		}
	})

	t.Run("stat upload on unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		_, err := service.StatUpload("unknown/repo", "i-dont-exist")
		AssertErrorIs(t, err, cascade.ErrBlobUploadUnknown)
	})

	// TODO: Write test to ensure that uploads are scoped to a repository.
}

func TestBlobUploadsMonolithic(t *testing.T) {
	service, _, _ := newTestRepository()

	t.Run("Monolithic blob upload - happy path", func(t *testing.T) {
		name := RandomName()
		digest, content := RandomBlob(32)

		session, err := service.InitUpload(name)
		RequireNoError(t, err)

		err = service.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content), 0)
		AssertNoError(t, err)

		err = service.CloseUpload(name, session.ID.String(), digest.String())
		AssertNoError(t, err)

		r, err := service.GetBlob(name, digest.String())
		AssertNoError(t, err)
		data, err := io.ReadAll(r)
		AssertNoError(t, err)
		AssertSlicesEqual(t, data, content)
	})

	t.Run("Uploading without a session returns ErrBlobUploadUknown", func(t *testing.T) {
		err := service.AppendUpload("fake", "abc", nil, 0)
		AssertErrorIs(t, err, cascade.ErrBlobUploadUnknown)
	})

	t.Run("Closing upload with invalid digest returns ErrDigestInvalid", func(t *testing.T) {
		name := RandomName()
		content := RandomContents(32)
		digest := "blablabla"

		session, err := service.InitUpload(name)
		RequireNoError(t, err)

		err = service.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content[0:16]), 0)
		RequireNoError(t, err)

		err = service.CloseUpload(name, session.ID.String(), digest)
		AssertErrorIs(t, err, cascade.ErrDigestInvalid)
	})

	t.Run("Closing upload with wrong digest returns ErrBlobUploadInvalid", func(t *testing.T) {
		name := RandomName()
		digest := RandomDigest()
		otherContent := RandomContents(32)

		session, err := service.InitUpload(name)
		RequireNoError(t, err)

		err = service.AppendUpload(name, session.ID.String(), bytes.NewBuffer(otherContent), 0)
		RequireNoError(t, err)

		err = service.CloseUpload(name, session.ID.String(), digest.String())
		AssertErrorIs(t, err, cascade.ErrBlobUploadInvalid)
	})
}

func TestServiceUpload(t *testing.T) {
	service, _, _ := newTestRepository()

	t.Run("written upload is retrievable", func(t *testing.T) {
		name := RandomName()
		digest, _, content := RandomManifest()

		session, err := service.InitUpload(name)
		RequireNoError(t, err)

		err = service.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content), 0)
		RequireNoError(t, err)

		err = service.CloseUpload(name, session.ID.String(), digest.String())
		RequireNoError(t, err)

		r, err := service.GetBlob(name, digest.String())
		AssertNoError(t, err)

		got, err := io.ReadAll(r)
		AssertNoError(t, err)
		AssertSlicesEqual(t, got, content)
	})

	t.Run("writing multiple times to same upload appends", func(t *testing.T) {
		repository := RandomName()
		content := RandomContents(32)

		session, err := service.InitUpload(repository)
		RequireNoError(t, err)

		err = service.AppendUpload(repository, session.ID.String(), bytes.NewBuffer(content[:16]), 0)
		AssertNoError(t, err)

		err = service.AppendUpload(repository, session.ID.String(), bytes.NewBuffer(content[16:]), 16)
		AssertNoError(t, err)

		info, err := service.StatUpload(repository, session.ID.String())
		AssertNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("unexpected upload size; got %d, want %d", got, want)
		}
	})

	t.Run("writing to unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		err := service.AppendUpload("1/2/3", "i-dont-exist", nil, 0)
		AssertErrorIs(t, err, cascade.ErrBlobUploadUnknown)
	})
}
