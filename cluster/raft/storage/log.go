package storage

import (
	"io"

	"go.etcd.io/raft/v3/raftpb"
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

	record := Record{Value: make([]byte, 128)}
	var cursor int64
	for {
		err := l.dec.Decode(&record)
		if err == io.EOF {
			break
		}

		cursor += int64(len(record.Value) + headerSize)
		l.entries = append(l.entries, cursor)
	}

	if len(l.entries) != 0 {
		l.file.Seek(l.entries[0], io.SeekStart)

		record := Record{Value: make([]byte, 128)}
		var entry raftpb.Entry
		l.dec.Decode(&record)
		switch record.Type {
		case TypeEntry:
			entry.Unmarshal(record.Value)
		default:
			panic("unknown type")
		}

		l.offset = int64(entry.Index)
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
func (l *Log) Append(entries []raftpb.Entry) {
	for _, entry := range entries {
		record := Record{Type: TypeEntry, Value: make([]byte, entry.Size())}
		entry.MarshalTo(record.Value)
		l.enc.Encode(record)
		l.entries = append(l.entries, int64(len(record.Value)+headerSize))
	}
}

// func (l *Log) Term(i uint64) (uint64, error)
// func (l *Log) LastIndex() (uint64, error)
// func (l *Log) FirstIndex() (uint64, error)
// func (l *Log) Snapshot() (raftpb.Snapshot, error)
