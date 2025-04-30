package server

import (
	"encoding/json"
	"errors"
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
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	// If the reference is a tag, fetch the digest first.
	if cascade.ValidateTag(reference) {
		var err error
		reference, err = s.service.GetTag(repository, reference)
		if err != nil {
			// TODO: Writes a body on a HEAD request while it shouldn't.
			writeErrorResponse(w, err)
			return
		}
	}

	info, err := s.service.StatManifest(repository, reference)
	if err != nil {
		// TODO: Writes a body on a HEAD request while it shouldn't.
		writeErrorResponse(w, err)
		return
	}

	w.Header().Set(HeaderContentLength, strconv.Itoa(int(info.Size)))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	// If the reference is a tag, fetch the digest first.
	if cascade.ValidateTag(reference) {
		reference, _ = s.service.GetTag(repository, reference)
	}

	meta, content, err := s.service.GetManifest(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	w.Header().Set(HeaderContentType, meta.MediaType)
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func (s *Server) putManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	digest := digest.FromBytes(data)

	if !cascade.ValidateTag(reference) {
		if digest.String() != reference {
			writeErrorResponse(w, cascade.ErrDigestInvalid)
			return
		}
	}

	subject, err := s.service.PutManifest(repository, digest.String(), data)
	switch err {
	default:
		w.WriteHeader(http.StatusInternalServerError)
		return
	case cascade.ErrManifestInvalid, cascade.ErrManifestBlobUnknown:
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(NewErrorResponse(err.(cascade.Error)))
		return
	case nil:
	}

	if subject != "" {
		w.Header().Set(HeaderOCISubject, subject.String())
	}

	if cascade.ValidateTag(reference) {
		err = s.service.PutTag(repository, reference, digest.String())
		if err != nil {
			writeErrorResponse(w, err)
			return
		}
	}

	w.Header().Set(HeaderLocation, fmt.Sprintf("/v2/%s/manifests/%s", repository, reference))
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) deleteManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	if cascade.ValidateTag(reference) {
		s.service.DeleteTag(repository, reference)

		w.WriteHeader(http.StatusAccepted)
	} else {
		err := s.service.DeleteManifest(repository, reference)
		if err != nil {
			if errors.Is(err, cascade.ErrManifestUnknown) {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(NewErrorResponse(err.(cascade.Error)))
				return
			}
		}

		w.WriteHeader(http.StatusAccepted)
	}
}
