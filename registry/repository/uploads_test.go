package repository

// import (
// 	"bytes"
// 	"io"
// 	"testing"

// 	. "github.com/robinkb/cascade/testing"
// )

// func (s *Suite) TestStatUpload() {
// 	if s.Tests.UploadsDisabled {
// 		s.T().SkipNow()
// 	}

// 	name := s.RandomRepository()

// 	s.T().Run("stat upload returns correct FileInfo", func(t *testing.T) {
// 		content := RandomBytes(32)

// 		session, err := s.repository.InitUpload(name)
// 		RequireNoError(t, err)
// 		err = s.repository.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content), 0)
// 		RequireNoError(t, err)

// 		info, err := s.repository.StatUpload(name, session.ID.String())
// 		AssertNoError(t, err)

// 		got := info.Size
// 		want := len(content)

// 		if info.Size != int64(len(content)) {
// 			t.Errorf("got unexpected upload size %d, want %d", got, want)
// 		}
// 	})

// 	s.T().Run("stat upload on unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
// 		_, err := s.repository.StatUpload(name, "i-dont-exist")
// 		AssertErrorIs(t, err, ErrBlobUploadUnknown)
// 	})

// 	// TODO: Write test to ensure that uploads are scoped to a repository.
// }

// func (s *Suite) TestBlobUploadsMonolithic() {
// 	if s.Tests.UploadsDisabled {
// 		s.T().SkipNow()
// 	}

// 	name := s.RandomRepository()

// 	s.T().Run("Monolithic blob upload - happy path", func(t *testing.T) {
// 		digest, content := RandomBlob(32)

// 		session, err := s.repository.InitUpload(name)
// 		RequireNoError(t, err)

// 		err = s.repository.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content), 0)
// 		AssertNoError(t, err)

// 		err = s.repository.CloseUpload(name, session.ID.String(), digest.String())
// 		AssertNoError(t, err)

// 		_, err = s.repository.StatUpload(name, session.ID.String())
// 		AssertErrorIs(t, err, ErrBlobUploadUnknown)

// 		r, err := s.repository.GetBlob(name, digest.String())
// 		RequireNoError(t, err)

// 		data, err := io.ReadAll(r)
// 		AssertNoError(t, err)
// 		AssertSlicesEqual(t, data, content)
// 	})

// 	s.T().Run("Uploading without a session returns ErrBlobUploadUknown", func(t *testing.T) {
// 		err := s.repository.AppendUpload(name, "abc", nil, 0)
// 		AssertErrorIs(t, err, ErrBlobUploadUnknown)
// 	})

// 	s.T().Run("Closing upload with invalid digest returns ErrDigestInvalid", func(t *testing.T) {
// 		content := RandomBytes(32)
// 		digest := "blablabla"

// 		session, err := s.repository.InitUpload(name)
// 		RequireNoError(t, err)

// 		err = s.repository.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content[0:16]), 0)
// 		RequireNoError(t, err)

// 		err = s.repository.CloseUpload(name, session.ID.String(), digest)
// 		AssertErrorIs(t, err, ErrDigestInvalid)
// 	})

// 	s.T().Run("Closing upload with wrong digest returns ErrBlobUploadInvalid", func(t *testing.T) {
// 		digest := RandomDigest()
// 		otherContent := RandomBytes(32)

// 		session, err := s.repository.InitUpload(name)
// 		RequireNoError(t, err)

// 		err = s.repository.AppendUpload(name, session.ID.String(), bytes.NewBuffer(otherContent), 0)
// 		RequireNoError(t, err)

// 		err = s.repository.CloseUpload(name, session.ID.String(), digest.String())
// 		AssertErrorIs(t, err, ErrBlobUploadInvalid)
// 	})
// }

// func (s *Suite) TestUploadOthers() {
// 	if s.Tests.UploadsDisabled {
// 		s.T().SkipNow()
// 	}

// 	s.T().Run("Initializing upload on unknown repository returns ErrNameUnknown", func(t *testing.T) {
// 		name := RandomName()

// 		_, err := s.repository.InitUpload(name)
// 		AssertErrorIs(t, err, ErrNameUnknown)
// 	})

// 	s.T().Run("Checking an upload on unknown repository returns ErrNameUnknown", func(t *testing.T) {
// 		name := RandomName()
// 		_, err := s.repository.StatUpload(name, "")
// 		AssertErrorIs(t, err, ErrNameUnknown)
// 	})
// }

// func (s *Suite) TestServiceUpload() {
// 	if s.Tests.UploadsDisabled {
// 		s.T().SkipNow()
// 	}

// 	name := s.RandomRepository()

// 	s.T().Run("written upload is retrievable", func(t *testing.T) {
// 		digest, _, content := RandomManifest()

// 		session, err := s.repository.InitUpload(name)
// 		RequireNoError(t, err)

// 		err = s.repository.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content), 0)
// 		RequireNoError(t, err)

// 		err = s.repository.CloseUpload(name, session.ID.String(), digest.String())
// 		RequireNoError(t, err)

// 		r, err := s.repository.GetBlob(name, digest.String())
// 		AssertNoError(t, err)

// 		got, err := io.ReadAll(r)
// 		AssertNoError(t, err)
// 		AssertSlicesEqual(t, got, content)
// 	})

// 	s.T().Run("writing multiple times to same upload appends", func(t *testing.T) {
// 		content := RandomBytes(32)

// 		session, err := s.repository.InitUpload(name)
// 		RequireNoError(t, err)

// 		err = s.repository.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content[:16]), 0)
// 		AssertNoError(t, err)

// 		err = s.repository.AppendUpload(name, session.ID.String(), bytes.NewBuffer(content[16:]), 16)
// 		AssertNoError(t, err)

// 		info, err := s.repository.StatUpload(name, session.ID.String())
// 		AssertNoError(t, err)

// 		got := info.Size
// 		want := int64(len(content))

// 		if got != want {
// 			t.Errorf("unexpected upload size; got %d, want %d", got, want)
// 		}
// 	})

// 	s.T().Run("writing to unknown upload returns ErrBlobUploadUnknown", func(t *testing.T) {
// 		err := s.repository.AppendUpload(name, "i-dont-exist", nil, 0)
// 		AssertErrorIs(t, err, ErrBlobUploadUnknown)
// 	})
// }
