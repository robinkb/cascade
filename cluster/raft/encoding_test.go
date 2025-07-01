package raft_test

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestEncodeDecode(t *testing.T) {
	want := randomRecord()
	buf := new(bytes.Buffer)

	err := raft.NewEncoder(buf).Encode(want)
	AssertNoError(t, err)

	got, err := raft.NewDecoder(buf).Decode()
	AssertNoError(t, err)
	AssertStructsEqual(t, got, want)
}

func TestEncodeDecodeErrorDetection(t *testing.T) {
	buf := new(bytes.Buffer)

	err := raft.NewEncoder(buf).Encode(randomRecord())
	AssertNoError(t, err)

	// Tamper with the written data.
	buf.Truncate(128)

	_, err = raft.NewDecoder(buf).Decode()
	AssertErrorIs(t, err, raft.ErrChecksumMismatch)
}

func randomRecord() raft.Record {
	return raft.Record{
		Type:  raft.RecordType(rand.Uint32()),
		Value: RandomContents(128),
	}
}
