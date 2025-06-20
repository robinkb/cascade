package transport

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/opencontainers/go-digest"
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
	tc := []struct {
		name string
		want []byte
	}{
		{
			"Data smaller than maximum payload",
			RandomContents(payloadMaxSize / 2),
		},
		{
			"Data exactly the maximum payload",
			RandomContents(payloadMaxSize),
		},
		{
			"Data larger than maximum payload",
			RandomContents(payloadMaxSize * 2),
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			wantID := MessageType(rand.UintN(1000))
			encoded, err := NewBufferedEncoder().EncodeStream(wantID, bytes.NewBuffer(tt.want))
			RequireNoError(t, err)

			gotID, decoded, err := NewBufferedDecoder().DecodeStream(encoded)
			RequireNoError(t, err)
			AssertEqual(t, gotID, wantID)

			gotData, err := io.ReadAll(decoded)
			RequireNoError(t, err)
			AssertSlicesEqual(t, gotData, tt.want)
		})
	}

}

func TestEncodeDecodeStreamLarge(t *testing.T) {
	t.Skip("long test, only run as a performance test")

	size := int64(1 << 30)
	encoder := NewBufferedEncoder()
	decoder := NewBufferedDecoder()

	wantID := MessageType(rand.UintN(1000))
	wantData := RandomStream(size)
	wantHash := sha256.New()
	tee := io.TeeReader(wantData, wantHash)

	encoded, err := encoder.EncodeStream(wantID, tee)
	RequireNoError(t, err)

	gotID, decoded, err := decoder.DecodeStream(encoded)
	RequireNoError(t, err)
	AssertEqual(t, gotID, wantID)

	gotHash := sha256.New()
	n, err := io.Copy(gotHash, decoded)
	RequireNoError(t, err)
	AssertEqual(t, n, size)
	AssertEqual(t,
		digest.NewDigest(digest.SHA256, gotHash),
		digest.NewDigest(digest.SHA256, wantHash),
	)
}

func TestEncodingDecodingDoesNotAllocate(t *testing.T) {
	encoder := NewBufferedEncoder()
	decoder := NewBufferedDecoder()

	t.Run("Ensure buffered encoding does not allocate", func(t *testing.T) {
		id := MessageType(rand.UintN(1000))
		data := RandomContents(128)

		allocs := testing.AllocsPerRun(10, func() {
			encoded, _ := encoder.Encode(id, data)
			decoder.Decode(encoded)
		})

		AssertEqual(t, allocs, 0)
	})

	t.Run("Ensure streaming encoding does not allocate more than necessary", func(t *testing.T) {
		mtype := MessageType(rand.UintN(1000))
		content := RandomContents(payloadMaxSize * 2)

		allocs := testing.AllocsPerRun(10, func() {
			data := bytes.NewBuffer(content)
			encoded, _ := encoder.EncodeStream(mtype, data)
			_, decoded, _ := decoder.DecodeStream(encoded)
			io.Copy(io.Discard, decoded)
		})

		// Allocates for these reasons:
		// 1. Creating the bytes.Buffer around the content
		// 2. Creating the streamEncoder when calling EncodeStream
		// 3. Creating the streamDecoder when calling DecodeStream
		AssertEqual(t, allocs, 3)
	})
}

func TestFlagsEncodeDecode(t *testing.T) {
	t.Run("Encoding stream flags", func(t *testing.T) {
		want := byte(0b10000000)

		f := flags{
			stream: true,
		}

		got := f.Byte()
		AssertEqual(t, got, want)
	})

	t.Run("Decoding stream flags", func(t *testing.T) {
		attr := byte(0b10000000)
		want := flags{
			stream: true,
		}
		got := parseFlags(attr)
		AssertEqual(t, got, want)
	})
}

func BenchmarkStreamingEncoding(b *testing.B) {
	size := int64(1 << 20)
	encoder := NewBufferedEncoder()
	decoder := NewBufferedDecoder()
	mtype := MessageType(rand.UintN(1000))
	content := RandomContents(size)

	for b.Loop() {
		data := bytes.NewBuffer(content)
		encoded, _ := encoder.EncodeStream(mtype, data)
		_, decoded, _ := decoder.DecodeStream(encoded)
		n, err := io.Copy(io.Discard, decoded)
		if n != size || err != nil {
			panic("test incorrect")
		}
	}
}
