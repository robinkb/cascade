package cascade

import (
	"encoding/json"

	godigest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func (s *registryService) ListReferrers(name, reference string) (*v1.Index, error) {
	digest, err := godigest.Parse(reference)
	if err != nil {
		return nil, ErrDigestInvalid
	}

	referrers, err := s.metadata.ListReferrers(name, digest)
	if err != nil {
		return nil, err
	}

	idx := &v1.Index{
		MediaType: v1.MediaTypeImageIndex,
		Manifests: make([]v1.Descriptor, len(referrers)),
	}

	for i, referrer := range referrers {
		path, err := s.metadata.GetManifest(name, referrer)
		if err != nil {
			return nil, err
		}

		data, err := s.blobs.Get(path)
		if err != nil {
			return nil, err
		}

		var manifest v1.Manifest
		err = json.Unmarshal(data, &manifest)
		if err != nil {
			return nil, err
		}

		digest := godigest.FromBytes(data)

		idx.Manifests[i] = v1.Descriptor{
			ArtifactType: manifest.ArtifactType,
			Digest:       digest,
			Size:         int64(len(data)),
			Annotations:  manifest.Annotations,
		}
	}

	return idx, nil
}
