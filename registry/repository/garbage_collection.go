package repository

import (
	"log"
	"slices"
	"sync"

	"github.com/opencontainers/go-digest"
)

func (s *repositoryService) collect(deleted []digest.Digest) {
	if len(deleted) == 0 {
		return
	}

	slices.Sort(deleted)
	deleted = slices.Compact(deleted)

	var wg sync.WaitGroup
	for _, id := range deleted {
		wg.Go(func() {
			if err := s.blobs.DeleteBlob(id); err != nil {
				log.Printf("failed to garbage collect blob with digest %s: %s", id, err)
			}
		})
	}
	wg.Wait()
}
