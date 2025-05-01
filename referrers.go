package cascade

import (
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func (s *registryService) ListReferrers(name, reference string) (*v1.Index, error) {
	digest, err := digest.Parse(reference)
	if err != nil {
		return nil, ErrDigestInvalid
	}

	referrers, err := s.metadata.ListReferrers(name, digest)
	if err != nil {
		return nil, err
	}

	idx := v1.Index{
		Manifests: make([]v1.Descriptor, 0),
	}

	for _, referrer := range referrers {
		meta, err := s.metadata.GetManifest(name, referrer)
		if err != nil {
			return nil, err
		}

		idx.Manifests = append(idx.Manifests, v1.Descriptor{
			Annotations: meta.Annotations,
			Digest:      referrer,
			Size:        meta.Size,
		})
	}

	return &idx, nil
}
