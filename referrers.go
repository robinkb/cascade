package cascade

import (
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func (s *registryService) ListReferrers(name, reference string) (*v1.Index, error) {
	_, err := digest.Parse(reference)
	if err != nil {
		return nil, ErrDigestInvalid
	}

	return nil, nil
}
