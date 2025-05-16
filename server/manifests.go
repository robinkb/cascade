package server

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/opencontainers/go-digest"
	"github.com/robinkb/cascade-registry"
)

func (s *Server) manifestsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		s.statManifestsHandler(w, r)
	case http.MethodGet:
		s.getManifestsHandler(w, r)
	case http.MethodPut:
		s.putManifestsHandler(w, r)
	case http.MethodDelete:
		s.deleteManifestsHandler(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) statManifestsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	repository, err := s.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	// If the reference is a tag, fetch the digest first.
	if cascade.ValidateTag(reference) {
		var err error
		reference, err = repository.GetTag(name, reference)
		if err != nil {
			errorHandler(w, r, err)
			return
		}
	}

	info, err := repository.StatManifest(name, reference)
	if err != nil {
		// TODO: Writes a body on a HEAD request while it shouldn't.
		errorHandler(w, r, err)
		return
	}

	w.Header().Set(HeaderContentLength, strconv.Itoa(int(info.Size)))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getManifestsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	repository, err := s.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	// If the reference is a tag, fetch the digest first.
	if cascade.ValidateTag(reference) {
		var err error
		reference, err = repository.GetTag(name, reference)
		if err != nil {
			errorHandler(w, r, err)
			return
		}
	}

	meta, content, err := repository.GetManifest(name, reference)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.Header().Set(HeaderContentType, meta.MediaType)
	w.WriteHeader(http.StatusOK)
	writeOrLog(w.Write(content))
}

func (s *Server) putManifestsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	repository, err := s.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	digest := digest.FromBytes(data)

	if !cascade.ValidateTag(reference) {
		if digest.String() != reference {
			errorHandler(w, r, cascade.ErrDigestInvalid)
			return
		}
	}

	subject, err := repository.PutManifest(name, digest.String(), data)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	if subject != "" {
		w.Header().Set(HeaderOCISubject, subject.String())
	}

	if cascade.ValidateTag(reference) {
		err = repository.PutTag(name, reference, digest.String())
		if err != nil {
			errorHandler(w, r, err)
			return
		}
	}

	w.Header().Set(HeaderLocation, fmt.Sprintf("/v2/%s/manifests/%s", name, reference))
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) deleteManifestsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reference := r.PathValue("reference")

	repository, err := s.service.GetRepository(name)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	if cascade.ValidateTag(reference) {
		err := repository.DeleteTag(name, reference)
		if err != nil {
			errorHandler(w, r, err)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		return
	}

	err = repository.DeleteManifest(name, reference)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)

}
