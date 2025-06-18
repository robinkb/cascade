package transport

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecodeMessage(t *testing.T) {
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

func TestEncodeDecodeStream(t *testing.T) {
	encoder := NewBufferedEncoder()
	decoder := NewBufferedDecoder()

	wantID := MessageType(rand.UintN(1000))
	wantData := RandomContents(128)
	wantBuf := bytes.NewBuffer(wantData)

	encoded, err := encoder.EncodeStream(wantID, wantBuf)
	RequireNoError(t, err)

	gotID, decoded, err := decoder.DecodeStream(encoded)
	RequireNoError(t, err)
	AssertEqual(t, gotID, wantID)

	gotData, err := io.ReadAll(decoded)
	RequireNoError(t, err)
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

func TestAttributesEncodeDecode(t *testing.T) {
	t.Run("Encoding stream attribute", func(t *testing.T) {
		want := byte(0b10000000)

		attr := attributes{
			stream: true,
		}

		got := attr.Byte()
		AssertEqual(t, got, want)
	})

	t.Run("Decoding stream attribute", func(t *testing.T) {
		attr := byte(0b10000000)
		want := attributes{
			stream: true,
		}
		got := parseAttributes(attr)
		AssertEqual(t, got, want)
	})
}
