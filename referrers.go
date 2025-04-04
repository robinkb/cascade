package cascade

import (
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
		Manifests: make([]v1.Descriptor, len(referrers)),
	}

	for i, _ := range referrers {
		// path, err := s.metadata.GetManifest(name, referrer)
		// if err != nil {
		// 	return nil, err
		// }

		// data, err := s.blobs.Get(path)
		// if err != nil {
		// 	return nil, err
		// }

		// var manifest v1.Manifest
		// err = json.Unmarshal(data, &manifest)
		// if err != nil {
		// 	return nil, err
		// }

		// digest := godigest.FromBytes(data)

		idx.Manifests[i] = v1.Descriptor{}
	}

	return idx, nil
}
