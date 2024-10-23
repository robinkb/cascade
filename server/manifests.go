package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
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

	info, err := s.service.StatManifest(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	w.Header().Set(headerContentLength, strconv.Itoa(int(info.Size)))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

	// If the reference is a tag, fetch the digest first.
	if cascade.ValidateTag(reference) {
		reference, _ = s.service.GetTag(repository, reference)
	}

	// TODO: This is doing too much. GetManifest should verify the Manifest,
	// and return the media type.
	var manifest v1.Manifest
	content, err := s.service.GetManifest(repository, reference)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}
	json.Unmarshal(content, &manifest)

	w.Header().Set(headerContentType, manifest.MediaType)
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

	err = s.service.PutManifest(repository, digest.String(), data)
	if err != nil {
		writeErrorResponse(w, err)
		return
	}

	if cascade.ValidateTag(reference) {
		err = s.service.PutTag(repository, reference, digest.String())
		if err != nil {
			writeErrorResponse(w, err)
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) deleteManifestsHandler(w http.ResponseWriter, r *http.Request) {
	repository := r.PathValue("repository")
	reference := r.PathValue("reference")

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
