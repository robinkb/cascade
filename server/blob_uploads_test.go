package server_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/gofrs/uuid/v5"
	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/robinkb/cascade-registry/testing/mock"
)

func TestBlobUploadsMonolithic(t *testing.T) {
	t.Run("Performing a monolithic upload", func(t *testing.T) {
		name, digest, content := RandomName(), RandomDigest(), RandomContents(32)
		sessionID := RandomString(8)

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			AppendUpload(name, sessionID, bytes.NewBuffer(content), int64(0)).
			Return(nil)
		repository.EXPECT().
			CloseUpload(name, sessionID, digest.String()).
			Return(nil)

		client := NewTestClientForRepository(t, name, repository)

		location := newLocation(name, sessionID)
		resp := client.CloseUploadWithContent(location, digest, content, 0)

		AssertResponseCode(t, resp, http.StatusCreated)
		AssertResponseHeaderSet(t, resp, "Location")
	})

	t.Run("Uploading without session returns 404", func(t *testing.T) {
		name, digest := RandomName(), RandomDigest()
		sessionID := "i-do-not-exist"

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			CloseUpload(name, sessionID, digest.String()).
			Return(cascade.ErrBlobUploadUnknown)

		client := NewTestClientForRepository(t, name, repository)

		location := newLocation(name, sessionID)
		resp := client.CloseUpload(location, digest)

		AssertResponseCode(t, resp, http.StatusNotFound)
		AssertResponseBodyContainsError(t, resp, cascade.ErrBlobUploadUnknown)
	})

	t.Run("Closing upload with content but without required headers returns 400", func(t *testing.T) {
		name, digest := RandomName(), RandomDigest()
		sessionID, _ := uuid.NewV7()

		location := newLocation(name, sessionID.String())
		query := location.Query()
		query.Add("digest", digest.String())
		location.RawQuery = query.Encode()

		client := NewTestClientForRepository(t, name, mock.NewRepositoryService(t))

		resp := client.Do(
			http.MethodPut,
			location.RequestURI(),
			nil,
			bytes.NewBuffer(RandomContents(32)),
		)

		AssertResponseCode(t, resp, http.StatusBadRequest)
	})

	t.Run("Closing upload without digest returns 400", func(t *testing.T) {
		name := RandomName()
		sessionID, _ := uuid.NewV7()
		location := newLocation(name, sessionID.String())

		client := NewTestClientForRepository(t, name, mock.NewRepositoryService(t))

		resp := client.Do(
			http.MethodPut,
			location.RequestURI(),
			nil,
			nil,
		)

		AssertResponseCode(t, resp, http.StatusBadRequest)
	})

	t.Run("Closing upload with invalid digest returns 400", func(t *testing.T) {
		name, id := RandomName(), "invalid"
		sessionID, _ := uuid.NewV7()
		location := newLocation(name, sessionID.String())

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			CloseUpload(name, sessionID.String(), id).
			Return(cascade.ErrDigestInvalid)

		client := NewTestClientForRepository(t, name, repository)

		resp := client.CloseUpload(location, digest.Digest(id))

		AssertResponseCode(t, resp, http.StatusBadRequest)
		AssertResponseBodyContainsError(t, resp, cascade.ErrDigestInvalid)
	})

	t.Run("Uploading with wrong digest returns 400", func(t *testing.T) {
		name, id := RandomName(), RandomDigest()
		sessionID, _ := uuid.NewV7()
		location := newLocation(name, sessionID.String())

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			CloseUpload(name, sessionID.String(), id.String()).
			Return(cascade.ErrBlobUploadInvalid)

		client := NewTestClientForRepository(t, name, repository)

		resp := client.CloseUpload(location, id)

		AssertResponseCode(t, resp, http.StatusBadRequest)
		AssertResponseBodyContainsError(t, resp, cascade.ErrBlobUploadInvalid)
	})
}

func TestBlobUploadsChunked(t *testing.T) {
	// TODO: This used to have integration-style tests that have been moved
	// to the conformance test. There should be more basic handler unit tests here.
}

func TestBlobUploadsStreamed(t *testing.T) {
	// TODO: This used to have integration-style tests that have been moved
	// to the conformance test. There should be more basic handler unit tests here.
	t.Run("Performing a streamed upload", func(t *testing.T) {
		name, content := RandomName(), RandomContents(32)
		sessionID := RandomString(6)

		repository := mock.NewRepositoryService(t)
		repository.EXPECT().
			AppendUpload(name, sessionID, mock.AnythingOfType("io.nopCloserWriterTo"), int64(0)).
			Return(nil)

		client := NewTestClientForRepository(t, name, repository)

		location := newLocation(name, sessionID)
		resp := client.UploadBlobStream(location, bytes.NewBuffer(content))
		AssertResponseCode(t, resp, http.StatusAccepted)
		AssertResponseHeaderSet(t, resp, "Location")
	})
}
