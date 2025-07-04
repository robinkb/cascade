package storage

import (
	"io"
	"log"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	TypeEntry RecordType = iota
	TypeHardState
	TypeSnapshot
)

// EntryPointer stores the offset of an Entry within the log's file.
// It also stores the Term that the Entry belongs to as an optimization.
// The Term of an Entry is queried very often by Raft, and we can keep it in-memory
// for a negligble cost, saving disk reads and record decodes.
type EntryPointer struct {
	Offset int64
	Term   uint64
}

func NewLog(r io.ReaderAt, w io.Writer) *Log {
	l := &Log{
		enc:     NewEncoder(w),
		dec:     NewDecoder(r),
		entries: make([]EntryPointer, 0),
	}

	// record := Record{Value: make([]byte, 128<<10)}
	// for {
	// 	n, err := l.dec.DecodeAt(&record, l.cursor)
	// 	if err == io.EOF {
	// 		break
	// 	}

	// 	l.entries = append(l.entries, l.cursor)
	// 	l.cursor += n
	// }

	// if len(l.entries) != 0 {
	// 	record := new(Record)
	// 	var entry raftpb.Entry
	// 	l.dec.DecodeAt(record, l.entries[0])
	// 	entry.Unmarshal(record.Value)

	// 	l.offset = entry.Index
	// }

	go func() {
		cs := &l.callStats
		for {
			time.Sleep(1 * time.Second)
			log.Printf(
				"[InitialState: %2d, Entries: %6d, Term: %6d, LastIndex: %7d, FirstIndex: %7d, Snapshot: %2d] [SetHardState: %6d, ApplySnapshot: %6d, Append: %6d, Compact: %6d]",
				cs.initialState, cs.entries, cs.term, cs.lastIndex, cs.firstIndex, cs.snapshot, cs.setHardState, cs.applySnapshot, cs.append, cs.compact,
			)
		}
	}()

	return l
}

type Log struct {
	enc Encoder
	dec Decoder

	hardState raftpb.HardState
	snapshot  raftpb.Snapshot

	// entries stores a series of pointers to Entries in the log file.
	// It is assumed that the Index of every Entry is sequential and without gaps.
	// It also stores each Entry's Term as an optimization.
	entries []EntryPointer
	// cursor tracks the position of our writes to the log file.
	// It is used to construct the location of Entries in the log file as stored
	// in the entries slice. The log is only ever appended, and cursor only ever increases.
	cursor int64
	// indexOffset stores the index of the oldest entry in the log.
	indexOffset uint64

	callStats struct {
		// part of the raft.Storage interface, called by Raft Node
		initialState int
		entries      int
		term         int
		lastIndex    int
		firstIndex   int
		snapshot     int

		// methods called by the application
		setHardState  int
		applySnapshot int
		append        int
		compact       int
	}
}

func (l *Log) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	l.callStats.initialState++
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
	l.callStats.entries++
	if lo < l.firstIndex() {
		return nil, raft.ErrCompacted
	}

	lo -= l.firstIndex()
	hi -= l.firstIndex()

	var size uint64
	var err error
	entries := make([]raftpb.Entry, 0)
	record := new(Record)

	for _, ptr := range l.entries[lo:hi] {
		_, err = l.dec.DecodeAt(record, ptr.Offset)
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
	// TODO: Term is being called REALLY often, but keeping terms in memory
	// along with indices would not increase memory usage much.
	// I should refactor this so that it doesn't have to go to disk.
	l.callStats.term++
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

	return l.entries[i].Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (l *Log) LastIndex() (uint64, error) {
	l.callStats.lastIndex++
	return l.lastIndex(), nil
}

func (l *Log) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.indexOffset
	}
	return l.firstIndex() + uint64(len(l.entries)) - 1
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot).
func (l *Log) FirstIndex() (uint64, error) {
	l.callStats.firstIndex++
	return l.firstIndex(), nil
}

func (l *Log) firstIndex() uint64 {
	// Makes no sense, but here we are. This is how Raft's MemoryStorage works.
	// The Raft Node will refuse to start up without it.
	if len(l.entries) == 0 {
		return 1
	}
	return l.indexOffset
}

func (l *Log) Snapshot() (raftpb.Snapshot, error) {
	l.callStats.snapshot++
	return l.snapshot, nil
}

func (l *Log) SetHardState(hardState raftpb.HardState) error {
	l.callStats.setHardState++
	// TODO: Persistence
	l.hardState = hardState
	return nil
}

func (l *Log) ApplySnapshot(snapshot raftpb.Snapshot) error {
	l.callStats.applySnapshot++
	// TODO: Persistence
	l.snapshot = snapshot
	return nil
}

func (l *Log) Append(entries []raftpb.Entry) error {
	l.callStats.append++
	if len(entries) == 0 {
		return nil
	}

	if len(l.entries) == 0 {
		l.indexOffset = entries[0].Index
	}

	var err error
	for _, entry := range entries {
		record := &Record{Type: TypeEntry, Value: make([]byte, entry.Size())}
		_, err = entry.MarshalTo(record.Value)
		panicOnErr(err)
		n, err := l.enc.Encode(record)
		panicOnErr(err)

		l.entries = append(l.entries, EntryPointer{
			Offset: l.cursor,
			Term:   entry.Term,
		})
		l.cursor += n
	}

	return nil
}

func (l *Log) Compact(i uint64) error {
	l.callStats.compact++
	return nil
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
