package cascade

import (
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ListReferrersOptions struct {
	ArtifactType string
}

type Referrers struct {
	Index          *v1.Index
	AppliedFilters []string
}

func (s *repositoryService) ListReferrers(name, reference string, opts *ListReferrersOptions) (*Referrers, error) {
	digest, err := digest.Parse(reference)
	if err != nil {
		return nil, ErrDigestInvalid
	}

	referrers, err := s.metadata.ListReferrers(name, digest)
	if err != nil {
		return nil, err
	}

	appliedFilters := make([]string, 0)
	if opts != nil && opts.ArtifactType != "" {
		appliedFilters = append(appliedFilters, "artifactType")
	}

	idx := v1.Index{
		Manifests: make([]v1.Descriptor, 0),
	}

	for _, referrer := range referrers {
		meta, err := s.metadata.GetManifest(name, referrer)
		if err != nil {
			return nil, err
		}

		if opts != nil && opts.ArtifactType != "" {
			if meta.ArtifactType != opts.ArtifactType {
				continue
			}
		}

		idx.Manifests = append(idx.Manifests, v1.Descriptor{
			Annotations:  meta.Annotations,
			ArtifactType: meta.ArtifactType,
			Digest:       referrer,
			MediaType:    meta.MediaType,
			Size:         meta.Size,
		})
	}

	return &Referrers{
		AppliedFilters: appliedFilters,
		Index:          &idx,
	}, nil
}
