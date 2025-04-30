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

	return &v1.Index{
		Manifests: referrers,
	}, nil
}
