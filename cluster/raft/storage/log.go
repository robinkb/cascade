package storage

import (
	"io"
)

const (
	TypeEntry RecordType = iota
	TypeHardState
	TypeSnapshot
)

func NewLog(r io.ReadSeeker, w io.Writer) *Log {
	l := &Log{
		file:    r,
		enc:     NewEncoder(w),
		dec:     NewDecoder(r),
		entries: make([]int64, 0),
	}

	var cursor int64
	for {
		rec, err := l.dec.Decode()
		if err == io.EOF {
			break
		}

		cursor += int64(len(rec.Value) + headerSize)
		l.entries = append(l.entries, cursor)
	}

	return l
}

type Log struct {
	file io.ReadSeeker
	enc  Encoder
	dec  Decoder

	entries []int64
	offset  int64
}

// func (l *Log) InitialState() (raftpb.HardState, raftpb.ConfState, error)

// func (l *Log) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error)
// func (l *Log) Term(i uint64) (uint64, error)
// func (l *Log) LastIndex() (uint64, error)
// func (l *Log) FirstIndex() (uint64, error)
// func (l *Log) Snapshot() (raftpb.Snapshot, error)
