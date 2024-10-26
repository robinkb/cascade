package cascade

import (
	"reflect"
	"testing"

	"github.com/robinkb/cascade-registry/paths"
)

func TestStatUpload(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("stat upload returns correct FileInfo", func(t *testing.T) {
		repository := "a/v/c"
		sessionID := "123"
		content := randomContents(32)

		store.Set(paths.MetaStore.UploadLink(repository, sessionID), nil)
		store.Set(paths.BlobStore.UploadData(sessionID), content)

		info, err := service.StatUpload(repository, sessionID)
		assertNoError(t, err)

		got := info.Size
		want := len(content)

		if info.Size != int64(len(content)) {
			t.Errorf("got unexpected upload size %d, want %d", got, want)
		}
	})

	t.Run("stat upload on unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		_, err := service.StatUpload("unknown/repo", "i-dont-exist")
		assertErrorIs(t, err, ErrBlobUploadUnknown)
	})
}

func TestBlobUploadsMonolithic(t *testing.T) {
	service := NewRegistryService(NewInMemoryStore())

	t.Run("Monolithic blob upload - happy path", func(t *testing.T) {
		name, digest, content := randomBlob(32)

		session := service.InitUpload(name)

		err := service.AppendUpload(name, session.ID, content, 0)
		assertNoError(t, err)

		err = service.CloseUpload(name, session.ID, digest.String())
		assertNoError(t, err)

		data, err := service.GetBlob(name, digest.String())
		assertContent(t, data, content)
		assertNoError(t, err)
	})

	t.Run("Uploading without a session returns ErrBlobUploadUknown", func(t *testing.T) {
		err := service.AppendUpload("fake", "abc", []byte{}, 0)
		assertErrorIs(t, err, ErrBlobUploadUnknown)
	})

	t.Run("Closing upload with invalid digest returns ErrDigestInvalid", func(t *testing.T) {
		name, _, content := randomBlob(32)
		digest := "blablabla"

		session := service.InitUpload(name)

		err := service.AppendUpload(name, session.ID, content[0:16], 0)
		assertNoError(t, err)

		err = service.CloseUpload(name, session.ID, digest)
		assertErrorIs(t, err, ErrDigestInvalid)
	})

	t.Run("Closing upload with wrong digest returns ErrBlobUploadInvalid", func(t *testing.T) {
		name, digest, _ := randomBlob(32)
		otherContent := randomContents(32)

		session := service.InitUpload(name)

		err := service.AppendUpload(name, session.ID, otherContent, 0)
		assertNoError(t, err)

		err = service.CloseUpload(name, session.ID, digest.String())
		assertErrorIs(t, err, ErrBlobUploadInvalid)
	})
}

func TestServiceUpload(t *testing.T) {
	store := NewInMemoryStore()
	service := NewRegistryService(store)

	t.Run("written upload is retrievable", func(t *testing.T) {
		repository := "a/b/c"
		content := randomContents(32)

		session := service.InitUpload(repository)

		err := service.AppendUpload(repository, session.ID, content, 0)
		assertNoError(t, err)

		got, err := store.Get(paths.BlobStore.UploadData(session.ID))
		assertNoError(t, err)

		if !reflect.DeepEqual(got, content) {
			t.Errorf("got unexpected byte content; not equal to written content")
		}
	})

	t.Run("writing multiple times to same upload appends", func(t *testing.T) {
		repository := "1/2/3"
		content := randomContents(32)

		session := service.InitUpload(repository)

		err := service.AppendUpload(repository, session.ID, content[:16], 0)
		assertNoError(t, err)

		err = service.AppendUpload(repository, session.ID, content[16:], 16)
		assertNoError(t, err)

		info, err := service.StatUpload(repository, session.ID)
		assertNoError(t, err)

		got := info.Size
		want := int64(len(content))

		if got != want {
			t.Errorf("unexpected upload size; got %d, want %d", got, want)
		}
	})

	t.Run("writing to unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
		err := service.AppendUpload("1/2/3", "i-dont-exist", []byte{}, 0)
		assertErrorIs(t, err, ErrBlobUploadUnknown)
	})
}
