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

	// If the reference is a tag, fetch the digest first.
	if cascade.ValidateTag(reference) {
		var err error
		reference, err = s.service.GetTag(name, reference)
		if err != nil {
			errorHandler(w, r, err)
			return
		}
	}

	info, err := s.service.StatManifest(name, reference)
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

	// If the reference is a tag, fetch the digest first.
	if cascade.ValidateTag(reference) {
		var err error
		reference, err = s.service.GetTag(name, reference)
		if err != nil {
			errorHandler(w, r, err)
			return
		}
	}

	meta, content, err := s.service.GetManifest(name, reference)
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

	subject, err := s.service.PutManifest(name, digest.String(), data)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	if subject != "" {
		w.Header().Set(HeaderOCISubject, subject.String())
	}

	if cascade.ValidateTag(reference) {
		err = s.service.PutTag(name, reference, digest.String())
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

	if cascade.ValidateTag(reference) {
		err := s.service.DeleteTag(name, reference)
		if err != nil {
			errorHandler(w, r, err)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		return
	}

	err := s.service.DeleteManifest(name, reference)
	if err != nil {
		errorHandler(w, r, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)

}
