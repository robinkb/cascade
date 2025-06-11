package cluster

import (
	"io"
	"math/rand/v2"
	"sync"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncoderDecoder(t *testing.T) {
	n := 1000
	r, w := io.Pipe()
	encoder := NewEncoder()
	decoder := NewDecoder()

	want := make([][]byte, n)
	for i := range n {
		want[i] = RandomContents(rand.Int64N(4096))
	}

	var wg sync.WaitGroup
	wg.Add(n)
	go func() {
		for i := range n {
			got, err := decoder.Decode(r)
			RequireNoError(t, err)
			AssertSlicesEqual(t, got, want[i])
			wg.Done()
		}
	}()

	for i := range n {
		err := encoder.Encode(w, want[i])
		RequireNoError(t, err)
	}

	wg.Wait()
}
