package transport

import (
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	encoder := NewBufferedEncoder()
	decoder := NewBufferedDecoder()

	wantID := MessageType(rand.UintN(1000))
	wantData := RandomContents(128)
	encoded, err := encoder.Encode(wantID, wantData)
	RequireNoError(t, err)

	gotID, gotData, err := decoder.Decode(encoded)
	RequireNoError(t, err)
	AssertEqual(t, gotID, wantID)
	AssertSlicesEqual(t, gotData, wantData)
}

func TestEncodingDecodingDoesNotAllocate(t *testing.T) {
	encoder := NewBufferedEncoder()
	decoder := NewBufferedDecoder()

	id := MessageType(rand.UintN(1000))
	data := RandomContents(128)

	allocs := testing.AllocsPerRun(100, func() {
		encoded, _ := encoder.Encode(id, data)
		decoder.Decode(encoded)
	})

	AssertEqual(t, allocs, 0)
}
