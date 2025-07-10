package storage

import (
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	TypeEntry RecordType = iota
	TypeHardState
	TypeSnapshot
)

func NewLogStorage(dir string) (*LogStorage, error) {
	l := &LogStorage{
		deck: NewDeck(dir, nil),
	}

	l.deck.ReadAll()

	if l.deck.Count(TypeEntry) > 0 {
		record := new(Record)
		err := l.deck.First(TypeEntry, record)
		if err != nil {
			return nil, err
		}

		var entry raftpb.Entry
		err = entry.Unmarshal(record.Value)
		if err != nil {
			return nil, err
		}

		l.indexOffset = entry.Index
	}

	// go func() {
	// 	cs := &l.callStats
	// 	for {
	// 		time.Sleep(1 * time.Second)
	// 		log.Printf(
	// 			"[InitialState: %2d, Entries: %6d, Term: %6d, LastIndex: %7d, FirstIndex: %7d, Snapshot: %2d] [SetHardState: %6d, ApplySnapshot: %6d, Append: %6d, Compact: %6d]",
	// 			cs.initialState, cs.entries, cs.term, cs.lastIndex, cs.firstIndex, cs.snapshot, cs.setHardState, cs.applySnapshot, cs.append, cs.compact,
	// 		)
	// 	}
	// }()

	return l, nil
}

// TODO: Sync. And hope performance doesn't tank.
type LogStorage struct {
	deck Deck

	hardState raftpb.HardState
	snapshot  raftpb.Snapshot

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

func (l *LogStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
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
func (l *LogStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	l.callStats.entries++
	if lo < l.firstIndex() {
		return nil, raft.ErrCompacted
	}

	lo -= l.firstIndex()
	hi -= l.firstIndex()

	var size uint64
	entries := make([]raftpb.Entry, 0)

	for record, err := range l.deck.Range(TypeEntry, int(lo), int(hi)) {
		if err != nil {
			return nil, err
		}

		size += uint64(len(record.Value))
		if size > maxSize && len(entries) != 0 {
			return entries, nil
		}

		var entry raftpb.Entry
		err = entry.Unmarshal(record.Value)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (l *LogStorage) Term(i uint64) (uint64, error) {
	l.callStats.term++
	if i == 0 && l.deck.Count(TypeEntry) == 0 {
		return 0, nil
	}
	if i > l.lastIndex() {
		return 0, raft.ErrUnavailable
	}
	if i < l.firstIndex() || l.deck.Count(TypeEntry) == 0 {
		return 0, raft.ErrCompacted
	}

	i -= l.firstIndex()

	// TODO: Optimize so that Term is read from memory again.
	r := new(Record)
	err := l.deck.Get(TypeEntry, int(i), r)
	if err != nil {
		return 0, err
	}

	var entry raftpb.Entry
	err = entry.Unmarshal(r.Value)
	if err != nil {
		return 0, err
	}

	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (l *LogStorage) LastIndex() (uint64, error) {
	l.callStats.lastIndex++
	return l.lastIndex(), nil
}

func (l *LogStorage) lastIndex() uint64 {
	if l.deck.Count(TypeEntry) == 0 {
		return l.indexOffset
	}
	return l.firstIndex() + uint64(l.deck.Count(TypeEntry)) - 1
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot).
func (l *LogStorage) FirstIndex() (uint64, error) {
	l.callStats.firstIndex++
	return l.firstIndex(), nil
}

func (l *LogStorage) firstIndex() uint64 {
	// Makes no sense, but here we are. This is how Raft's MemoryStorage works.
	// The Raft Node will refuse to start up without it.
	if l.deck.Count(TypeEntry) == 0 {
		return 1
	}
	return l.indexOffset
}

func (l *LogStorage) Snapshot() (raftpb.Snapshot, error) {
	l.callStats.snapshot++
	return l.snapshot, nil
}

func (l *LogStorage) SetHardState(hardState raftpb.HardState) error {
	l.callStats.setHardState++

	record := &Record{
		Type:  TypeHardState,
		Value: make([]byte, hardState.Size()),
	}
	_, err := hardState.MarshalTo(record.Value)
	if err != nil {
		return err
	}

	err = l.deck.Append(record)
	if err != nil {
		return err
	}

	l.hardState = hardState
	return nil
}

func (l *LogStorage) ApplySnapshot(snapshot raftpb.Snapshot) error {
	l.callStats.applySnapshot++

	record := &Record{
		Type:  TypeSnapshot,
		Value: make([]byte, snapshot.Size()),
	}
	_, err := snapshot.MarshalTo(record.Value)
	if err != nil {
		return err
	}

	err = l.deck.Append(record)
	if err != nil {
		return err
	}

	l.snapshot = snapshot
	return nil
}

func (l *LogStorage) Append(entries []raftpb.Entry) error {
	l.callStats.append++
	if len(entries) == 0 {
		return nil
	}

	if l.deck.Count(TypeEntry) == 0 {
		l.indexOffset = entries[0].Index
	}

	var err error
	for _, entry := range entries {
		record := &Record{Type: TypeEntry, Value: make([]byte, entry.Size())}
		_, err = entry.MarshalTo(record.Value)
		if err != nil {
			return err
		}

		err = l.deck.Append(record)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *LogStorage) Compact(i uint64) error {
	l.callStats.compact++
	return nil
}
