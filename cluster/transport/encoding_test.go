package transport

import (
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	encoder := NewBufferedEncoder()
	decoder := NewBufferedDecoder()

	wantID := OperationID(rand.UintN(1000))
	wantData := RandomContents(128)
	encoded, err := encoder.Encode(wantID, wantData)
	RequireNoError(t, err)

	gotID, gotData, err := decoder.Decode(encoded)
	RequireNoError(t, err)
	AssertEqual(t, gotID, wantID)
	AssertSlicesEqual(t, gotData, wantData)
}

func BenchmarkEncodeDecode(b *testing.B) {
	encoder := NewBufferedEncoder()
	decoder := NewBufferedDecoder()

	id := OperationID(rand.UintN(1000))
	data := RandomContents(128)

	for b.Loop() {
		encoded, _ := encoder.Encode(id, data)
		decoder.Decode(encoded)
	}
}
