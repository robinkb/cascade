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

func NewLog(r io.ReaderAt, w io.Writer) *Log {
	l := &Log{
		enc:     NewEncoder(w),
		dec:     NewDecoder(r),
		entries: make([]int64, 0),
	}

	record := Record{Value: make([]byte, 128<<10)}
	for {
		n, err := l.dec.DecodeAt(&record, l.cursor)
		if err == io.EOF {
			break
		}

		l.entries = append(l.entries, l.cursor)
		l.cursor += n
	}

	if len(l.entries) != 0 {
		record := new(Record)
		var entry raftpb.Entry
		l.dec.DecodeAt(record, l.entries[0])
		entry.Unmarshal(record.Value)

		l.offset = entry.Index
	}

	return l
}

type Log struct {
	enc Encoder
	dec Decoder

	hardState raftpb.HardState
	snapshot  raftpb.Snapshot

	entries []int64
	cursor  int64
	offset  uint64
}

func (l *Log) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return l.hardState, l.snapshot.Metadata.ConfState, nil
}

// Entries returns a slice of consecutive log entries in the range [lo, hi),
// starting from lo. The maxSize limits the total size of the log entries
// returned, but Entries returns at least one entry if any.
//
// The caller of Entries owns the returned slice, and may append to it. The
// individual entries in the slice must not be mutated, neither by the Storage
// implementation nor the caller. Note that raft may forward these entries
// back to the application via Ready struct, so the corresponding handler must
// not mutate entries either (see comments in Ready struct).
//
// Since the caller may append to the returned slice, Storage implementation
// must protect its state from corruption that such appends may cause. For
// example, common ways to do so are:
//   - allocate the slice before returning it (safest option),
//   - return a slice protected by Go full slice expression, which causes
//     copying on appends (see MemoryStorage).
//
// Returns ErrCompacted if entry lo has been compacted, or ErrUnavailable if
// encountered an unavailable entry in [lo, hi).
func (l *Log) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if lo < l.firstIndex() {
		return nil, raft.ErrCompacted
	}

	lo -= l.firstIndex()
	hi -= l.firstIndex()

	var size uint64
	var err error
	entries := make([]raftpb.Entry, 0)
	record := new(Record)

	for _, pos := range l.entries[lo:hi] {
		_, err = l.dec.DecodeAt(record, pos)
		panicOnErr(err)

		size += uint64(len(record.Value))
		if size > maxSize && len(entries) != 0 {
			return entries, nil
		}

		var entry raftpb.Entry
		err = entry.Unmarshal(record.Value)
		panicOnErr(err)

		entries = append(entries, entry)
	}

	return entries, nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (l *Log) Term(i uint64) (uint64, error) {
	if i == 0 && len(l.entries) == 0 {
		return 0, nil
	}
	if i > l.lastIndex() {
		return 0, raft.ErrUnavailable
	}
	if i < l.firstIndex() || len(l.entries) == 0 {
		return 0, raft.ErrCompacted
	}

	i -= l.firstIndex()

	var err error
	record := Record{Value: make([]byte, 128<<10)}
	_, err = l.dec.DecodeAt(&record, l.entries[i])
	panicOnErr(err)

	var entry raftpb.Entry
	err = entry.Unmarshal(record.Value)
	panicOnErr(err)

	return entry.Index, nil
}

// LastIndex returns the index of the last entry in the log.
func (l *Log) LastIndex() (uint64, error) {
	return l.lastIndex(), nil
}

func (l *Log) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.offset
	}
	return l.firstIndex() + uint64(len(l.entries)) - 1
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot).
func (l *Log) FirstIndex() (uint64, error) {
	return l.firstIndex(), nil
}

func (l *Log) firstIndex() uint64 {
	// Makes no sense, but here we are. This is how Raft's MemoryStorage works.
	// The Raft Node will refuse to start up without it.
	if len(l.entries) == 0 {
		return 1
	}
	return l.offset
}

func (l *Log) Snapshot() (raftpb.Snapshot, error) {
	return l.snapshot, nil
}

func (l *Log) SetHardState(hardState raftpb.HardState) error {
	// TODO: Persistence
	l.hardState = hardState
	return nil
}

func (l *Log) ApplySnapshot(snapshot raftpb.Snapshot) error {
	// TODO: Persistence
	l.snapshot = snapshot
	return nil
}

func (l *Log) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	if len(l.entries) == 0 {
		l.offset = entries[0].Index
	}

	var err error
	for _, entry := range entries {
		record := &Record{Type: TypeEntry, Value: make([]byte, entry.Size())}
		_, err = entry.MarshalTo(record.Value)
		panicOnErr(err)
		n, err := l.enc.Encode(record)
		panicOnErr(err)
		l.entries = append(l.entries, l.cursor)
		l.cursor += n
	}

	return nil
}

func (l *Log) Compact(i uint64) error {
	return nil
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
