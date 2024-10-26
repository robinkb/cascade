package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
)

func TestBlobUploadsMonolithic(t *testing.T) {
	service := cascade.NewRegistryService(cascade.NewInMemoryStore())
	server := New(service)

	t.Run("Monolithic blob upload - happy path", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)

		request := newBlobUploadRequest(session.Location, content)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)
		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})

	t.Run("Uploading without session returns 404", func(t *testing.T) {
		server := New(&StubRegistryService{closeUpload: func(repository, id, digest string) error {
			return cascade.ErrBlobUploadUnknown
		}})

		request := newBlobUploadRequest("/v2/library/fedora/blobs/uploads/123", nil)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusNotFound)
		assertErrorInResponseBody(t, response.Body, cascade.ErrBlobUploadUnknown)
	})

	t.Run("Uploading without required headers returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newBlobUploadRequest(session.Location, content)
		request.Header.Del(headerContentType)
		request.Header.Del(headerContentLength)
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Uploading without digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newBlobUploadRequest(session.Location, content)
		request.URL.RawQuery = ""
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Uploading with invalid digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newBlobUploadRequest(session.Location, content)
		request.URL.RawQuery = "digest=blablabla"
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
	})

	t.Run("Uploading with wrong digest returns 400", func(t *testing.T) {
		session := server.service.InitUpload("library/fedora")
		content := randomContents(32)
		request := newBlobUploadRequest(session.Location, content)
		response := httptest.NewRecorder()

		otherContent := randomContents(64)
		id := digest.FromBytes(otherContent)
		query := request.URL.Query()
		query.Set("digest", id.String())
		request.URL.RawQuery = query.Encode()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusBadRequest)
		assertErrorInResponseBody(t, response.Body, cascade.ErrBlobUploadInvalid)
	})
}

func TestBlobUploadsChunked(t *testing.T) {
	service := cascade.NewRegistryService(cascade.NewInMemoryStore())
	server := New(service)

	t.Run("Chunked upload happy path", func(t *testing.T) {
		// Initialize the upload session by obtaining an ID.
		// For chunked uploads, header Content-Length: 0 must be set.
		request := newInitUploadRequest("library/fedora")
		request.Header.Add(headerContentLength, "0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)

		content := randomContents(16 * 1024)
		digest := digest.FromBytes(content)
		r := bytes.NewReader(content)
		buffer := make([]byte, 1*1024)
		written := 0

		for {
			n, err := io.ReadFull(r, buffer)
			assertNoError(t, err)

			request = newUploadChunkRequest(location, buffer, written)
			response = httptest.NewRecorder()

			server.ServeHTTP(response, request)

			written += n

			assertStatus(t, response.Code, http.StatusAccepted)
			assertHeaderSet(t, headerLocation, response.Header())
			assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))

			if r.Len() == 0 {
				break
			}

			location = response.Header().Get(headerLocation)
		}

		request = newCloseUploadRequest(location, digest.String(), nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location = response.Header().Get(headerLocation)

		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})

	t.Run("Chunked upload with content in the closing call", func(t *testing.T) {
		request := newInitUploadRequest("library/fedora")
		request.Header.Add(headerContentLength, "0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)

		location := response.Header().Get(headerLocation)

		// Prepare content to upload
		content := randomContents(2 * 1024)
		digest := digest.FromBytes(content)
		r := bytes.NewReader(content)
		buffer := make([]byte, 1*1024)
		written := 0

		n, err := io.ReadFull(r, buffer)
		assertNoError(t, err)

		// Do an upload
		request = newUploadChunkRequest(location, buffer, written)
		response = httptest.NewRecorder()
		server.ServeHTTP(response, request)
		written += n

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))

		// Close the upload with the final chunk.
		_, err = io.ReadFull(r, buffer)
		assertNoError(t, err)

		request = newCloseUploadRequest(location, digest.String(), buffer)
		request.Header.Set(headerContentType, contentTypeOctetStream)
		request.Header.Set(headerContentLength, strconv.Itoa((len(buffer))))
		request.Header.Set(headerContentRange, fmt.Sprintf("%d-%d", written, written+len(buffer)))
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location = response.Header().Get(headerLocation)

		// Verify that the content was uploaded successfully.
		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})

	t.Run("Chunked upload with dyscalculic client (gets the ranges wrong)", func(t *testing.T) {
		request := newInitUploadRequest("library/fedora")
		request.Header.Add(headerContentLength, "0")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)

		location := response.Header().Get(headerLocation)

		// Prepare content to upload
		content := randomContents(2 * 1024)
		digest := digest.FromBytes(content)
		r := bytes.NewReader(content)
		buffer := make([]byte, 1*1024)
		written := 0

		n, err := io.ReadFull(r, buffer)
		assertNoError(t, err)

		// Do a proper upload
		request = newUploadChunkRequest(location, buffer, written)
		response = httptest.NewRecorder()
		server.ServeHTTP(response, request)
		written += n

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))

		_, err = io.ReadFull(r, buffer)
		assertNoError(t, err)

		request = newUploadChunkRequest(location, buffer, written)
		// Mess up the start of the content range
		request.Header.Set(headerContentRange, fmt.Sprintf("1-%d", written))
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusRequestedRangeNotSatisfiable)

		// Check our upload status, confirm nothing is written.
		request = newCheckUploadRequest(location)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))
		r.Seek(int64(written), 0)

		// Try uploading the chunk again, this time missing up
		// the end of the content range.
		_, err = io.ReadFull(r, buffer)
		assertNoError(t, err)

		request = newUploadChunkRequest(location, buffer, written)
		request.Header.Set(headerContentRange, fmt.Sprintf("%d-1", written))
		response = httptest.NewRecorder()
		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusRequestedRangeNotSatisfiable)

		// Check our upload status, confirm nothing is written.
		request = newCheckUploadRequest(location)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))
		r.Seek(int64(written), 0)

		// Do it properly this time.
		n, err = io.ReadFull(r, buffer)
		assertNoError(t, err)

		request = newUploadChunkRequest(location, buffer, written)
		response = httptest.NewRecorder()
		server.ServeHTTP(response, request)
		written += n

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeader(t, headerRange, response.Header(), fmt.Sprintf("0-%d", written))

		// And close the upload.
		request = newCloseUploadRequest(location, digest.String(), nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location = response.Header().Get(headerLocation)

		// Verify that the content was uploaded successfully.
		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})
}

func TestBlobUploadsStreamed(t *testing.T) {
	service := cascade.NewRegistryService(cascade.NewInMemoryStore())
	server := New(service)

	t.Run("Streamed upload happy path", func(t *testing.T) {
		// Initialize the upload session by obtaining an ID.
		request := newInitUploadRequest("library/fedora")
		response := httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)
		assertHeaderSet(t, headerLocation, response.Header())

		location := response.Header().Get(headerLocation)

		content := randomContents(32 * 1024)
		digest := digest.FromBytes(content)
		r := bytes.NewReader(content)

		request, _ = http.NewRequest(http.MethodPatch, location, r)
		request.Header.Set(headerContentType, contentTypeOctetStream)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusAccepted)

		request = newCloseUploadRequest(location, digest.String(), nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusCreated)
		assertHeaderSet(t, headerLocation, response.Header())

		location = response.Header().Get(headerLocation)

		request, _ = http.NewRequest(http.MethodGet, location, nil)
		response = httptest.NewRecorder()

		server.ServeHTTP(response, request)

		assertStatus(t, response.Code, http.StatusOK)
		assertResponseBody(t, response.Body.Bytes(), content)
	})
}

func newBlobUploadRequest(location string, content []byte) *http.Request {
	id := digest.FromBytes(content)

	req, _ := http.NewRequest(http.MethodPut, location, bytes.NewBuffer(content))
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerContentLength, fmt.Sprint(len(content)))

	query := req.URL.Query()
	query.Set("digest", id.String())
	req.URL.RawQuery = query.Encode()
	return req
}

func newCheckUploadRequest(location string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, location, nil)
	return req
}

func newUploadChunkRequest(location string, content []byte, written int) *http.Request {
	size := len(content)
	buf := bytes.NewBuffer(content)
	req, _ := http.NewRequest(http.MethodPatch, location, buf)
	req.Header.Set(headerContentType, contentTypeOctetStream)
	req.Header.Set(headerContentRange, fmt.Sprintf("%d-%d", written, written+size))
	req.Header.Set(headerContentLength, strconv.Itoa(size))
	return req
}

func newCloseUploadRequest(location, digest string, content []byte) *http.Request {
	var body io.Reader
	if len(content) > 0 {
		body = bytes.NewBuffer(content)
	}

	req, _ := http.NewRequest(http.MethodPut, location, body)
	query := req.URL.Query()
	query.Set("digest", digest)
	req.URL.RawQuery = query.Encode()
	return req
}
