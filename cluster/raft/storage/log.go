package storage

import (
	"io"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	TypeEntry RecordType = iota
	TypeHardState
	TypeSnapshot
)

func NewLog(r io.ReadSeeker, w io.Writer) *Log {
	l := &Log{
		enc:     NewEncoder(w),
		dec:     NewDecoder(r),
		entries: make([]int64, 0),
	}

	record := Record{Value: make([]byte, 128)}
	for {
		n, err := l.dec.Decode(&record)
		if err == io.EOF {
			break
		}

		l.entries = append(l.entries, l.cursor)
		l.cursor += n
	}

	if len(l.entries) != 0 {
		l.dec.Seek(l.entries[0], io.SeekStart)

		record := Record{Value: make([]byte, 128)}
		var entry raftpb.Entry
		l.dec.Decode(&record)
		entry.Unmarshal(record.Value)

		l.offset = entry.Index
	}

	return l
}

type Log struct {
	enc Encoder
	dec Decoder

	entries []int64
	cursor  int64
	offset  uint64
}

// func (l *Log) InitialState() (raftpb.HardState, raftpb.ConfState, error)

func (l *Log) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if lo <= l.offset {
		return nil, raft.ErrCompacted
	}
	if hi > uint64(len(l.entries))+l.offset {
		return nil, raft.ErrUnavailable
	}

	lo -= l.offset
	hi -= l.offset

	var size uint64
	entries := make([]raftpb.Entry, 0)
	record := Record{Value: make([]byte, 128)}

	for _, pos := range l.entries[lo:hi] {
		l.dec.Seek(pos, io.SeekStart)
		l.dec.Decode(&record)

		size += uint64(len(record.Value))
		if size > maxSize && len(entries) != 0 {
			return entries, nil
		}

		var entry raftpb.Entry
		entry.Unmarshal(record.Value)
		entries = append(entries, entry)
	}

	return entries, nil
}

func (l *Log) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	if len(l.entries) == 0 {
		l.offset = entries[0].Index
	}

	for _, entry := range entries {
		record := Record{Type: TypeEntry, Value: make([]byte, entry.Size())}
		entry.MarshalTo(record.Value)
		n, _ := l.enc.Encode(record)
		l.entries = append(l.entries, l.cursor)
		l.cursor += n
	}

	return nil
}

func (l *Log) Term(i uint64) (uint64, error) {
	if i < l.offset {
		return 0, raft.ErrCompacted
	}
	if i >= uint64(len(l.entries))+l.offset {
		return 0, raft.ErrUnavailable
	}

	i -= l.offset

	record := Record{Value: make([]byte, 128)}
	l.dec.Seek(l.entries[i], io.SeekStart)
	l.dec.Decode(&record)

	var entry raftpb.Entry
	entry.Unmarshal(record.Value)

	return entry.Index, nil
}

func (l *Log) LastIndex() (uint64, error) {
	return l.offset + uint64(len(l.entries)) - 1, nil
}

func (l *Log) FirstIndex() (uint64, error) {
	return l.offset + 1, nil
}

// func (l *Log) Snapshot() (raftpb.Snapshot, error)
