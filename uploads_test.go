package cascade_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/robinkb/cascade-registry"
)

func TestStatUpload(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("stat upload returns correct FileInfo", func(t *testing.T) {
		repository := "a/v/c"
		content := randomContents(32)

		session := service.InitUpload(repository)
		err := service.AppendUpload(repository, session.ID.String(), bytes.NewBuffer(content), 0)
		assertNoError(t, err)

		info, err := service.StatUpload(repository, session.ID.String())
		assertNoError(t, err)

		got := info.Size
		want := len(content)

		if info.Size != int64(len(content)) {
			t.Errorf("got unexpected upload size %d, want %d", got, want)
		}
	})

	t.Run("stat upload on unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		_, err := service.StatUpload("unknown/repo", "i-dont-exist")
		assertErrorIs(t, err, cascade.ErrBlobUploadUnknown)
	})

	// TODO: Write test to ensure that uploads are scoped to a repository.
}

func TestBlobUploadsMonolithic(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("Monolithic blob upload - happy path", func(t *testing.T) {
		name, digest, content := randomBlob(32)

		session := service.InitUpload(name)

		err := service.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content), 0)
		assertNoError(t, err)

		err = service.CloseUpload(name, session.ID.String(), digest.String())
		assertNoError(t, err)

		r, err := service.GetBlob(name, digest.String())
		assertNoError(t, err)
		data, err := io.ReadAll(r)
		assertNoError(t, err)
		assertContent(t, data, content)
	})

	t.Run("Uploading without a session returns ErrBlobUploadUknown", func(t *testing.T) {
		err := service.AppendUpload("fake", "abc", nil, 0)
		assertErrorIs(t, err, cascade.ErrBlobUploadUnknown)
	})

	t.Run("Closing upload with invalid digest returns ErrDigestInvalid", func(t *testing.T) {
		name, _, content := randomBlob(32)
		digest := "blablabla"

		session := service.InitUpload(name)

		err := service.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content[0:16]), 0)
		assertNoError(t, err)

		err = service.CloseUpload(name, session.ID.String(), digest)
		assertErrorIs(t, err, cascade.ErrDigestInvalid)
	})

	t.Run("Closing upload with wrong digest returns ErrBlobUploadInvalid", func(t *testing.T) {
		name, digest, _ := randomBlob(32)
		otherContent := randomContents(32)

		session := service.InitUpload(name)

		err := service.AppendUpload(name, session.ID.String(), bytes.NewBuffer(otherContent), 0)
		assertNoError(t, err)

		err = service.CloseUpload(name, session.ID.String(), digest.String())
		assertErrorIs(t, err, cascade.ErrBlobUploadInvalid)
	})
}

func TestServiceUpload(t *testing.T) {
	service, _, _ := newTestRegistry()

	t.Run("written upload is retrievable", func(t *testing.T) {
		name, digest, content := randomManifest()

		session := service.InitUpload(name)

		err := service.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content), 0)
		assertNoError(t, err)

		err = service.CloseUpload(name, session.ID.String(), digest.String())
		assertNoError(t, err)

		r, err := service.GetBlob(name, digest.String())
		assertNoError(t, err)

		got, err := io.ReadAll(r)
		assertNoError(t, err)

		if !bytes.Equal(got, content) {
			t.Errorf("got unexpected byte content; not equal to written content")
		}
	})

	t.Run("writing multiple times to same upload appends", func(t *testing.T) {
		repository := "1/2/3"
		content := randomContents(32)

		session := service.InitUpload(repository)

		err := service.AppendUpload(repository, session.ID.String(), bytes.NewBuffer(content[:16]), 0)
		assertNoError(t, err)

		err = service.AppendUpload(repository, session.ID.String(), bytes.NewBuffer(content[16:]), 16)
		assertNoError(t, err)

		info, err := service.StatUpload(repository, session.ID.String())
		assertNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("unexpected upload size; got %d, want %d", got, want)
		}
	})

	t.Run("writing to unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		err := service.AppendUpload("1/2/3", "i-dont-exist", nil, 0)
		assertErrorIs(t, err, cascade.ErrBlobUploadUnknown)
	})
}
