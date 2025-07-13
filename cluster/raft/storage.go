package raft

import (
	"fmt"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// TODO: Types of Records being saved to the log.
	// Any changes to the values of these will break storage compatibility.
	// So maybe I should use strings instead. My budget for entry types is uint32.
	// 32 characters should be plenty. Type casting strings straight from
	// bytes should not be any more expensive than unsigned integers.
	TypeEntry storage.RecordType = iota
	TypeHardState
	TypeSnapshot
)

func NewDiskStorage(dir string, c *storage.DeckConfig) (*DiskStorage, error) {
	l := &DiskStorage{
		deck: storage.NewDeck(dir, c),
	}

	l.deck.ReadAll()

	record := new(storage.Record)
	if l.deck.Count(TypeEntry) > 0 {
		err := l.deck.First(TypeEntry, record)
		if err != nil {
			return nil, err
		}

		var entry raftpb.Entry
		err = entry.Unmarshal(record.Value)
		if err != nil {
			return nil, err
		}

		l.firstEntry = raftpb.Entry{
			Term:  entry.Term,
			Index: entry.Index,
		}
	}

	if l.deck.Count(TypeHardState) > 0 {
		err := l.deck.Last(TypeHardState, record)
		if err != nil {
			return nil, err
		}

		var hardState raftpb.HardState
		err = hardState.Unmarshal(record.Value)
		if err != nil {
			return nil, err
		}

		l.hardState = hardState
	}

	if l.deck.Count(TypeSnapshot) > 0 {
		err := l.deck.Last(TypeSnapshot, record)
		if err != nil {
			return nil, err
		}

		var snapshot raftpb.Snapshot
		err = snapshot.Unmarshal(record.Value)
		if err != nil {
			return nil, err
		}

		l.snapshot = snapshot
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

	l.deck.CompactionHandler(l.compactionHandler())

	return l, nil
}

// TODO: Sync. And hope performance doesn't tank.
type DiskStorage struct {
	deck storage.Deck

	hardState raftpb.HardState
	snapshot  raftpb.Snapshot

	firstEntry     raftpb.Entry
	compactedEntry raftpb.Entry

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

func (l *DiskStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
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
func (l *DiskStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	l.callStats.entries++

	fi := l.firstIndex()
	if lo < fi {
		return nil, raft.ErrCompacted
	}

	lo -= fi
	hi -= fi

	var size uint64
	entries := make([]raftpb.Entry, 0)

	for record, err := range l.deck.Range(TypeEntry, int(lo), int(hi)) {
		if err != nil {
			return nil, fmt.Errorf("failed to get entries [lo: %d] [hi: %d]: %w", lo, hi, err)
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
func (l *DiskStorage) Term(i uint64) (uint64, error) {
	l.callStats.term++
	if i == 0 && l.deck.Count(TypeEntry) == 0 {
		return 0, nil
	}

	li := l.lastIndex()
	if i > li {
		return 0, raft.ErrUnavailable
	}

	fi := l.firstIndex()
	if i == fi-1 {
		return l.compactedEntry.Term, nil
	}
	if i < fi || l.deck.Count(TypeEntry) == 0 {
		return 0, raft.ErrCompacted
	}

	i -= fi

	r := new(storage.Record)
	err := l.deck.Get(TypeEntry, int(i), r)
	if err != nil {
		return 0, fmt.Errorf("failed to get term [i: %d] [fi: %d] [li: %d]: %w", i, fi, li, err)
	}

	var entry raftpb.Entry
	err = entry.Unmarshal(r.Value)
	if err != nil {
		return 0, err
	}

	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (l *DiskStorage) LastIndex() (uint64, error) {
	l.callStats.lastIndex++
	return l.lastIndex(), nil
}

func (l *DiskStorage) lastIndex() uint64 {
	if l.deck.Count(TypeEntry) == 0 {
		return l.firstEntry.Index
	}
	return l.firstIndex() + uint64(l.deck.Count(TypeEntry)) - 1
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot).
func (l *DiskStorage) FirstIndex() (uint64, error) {
	l.callStats.firstIndex++
	return l.firstIndex(), nil
}

func (l *DiskStorage) firstIndex() uint64 {
	// Makes no sense, but here we are. This is how Raft's MemoryStorage works.
	// The Raft Node will refuse to start up without it.
	if l.deck.Count(TypeEntry) == 0 {
		return 1
	}
	return l.firstEntry.Index
}

// Snapshot returns the latest snapshot persisted to storage.
func (l *DiskStorage) Snapshot() (raftpb.Snapshot, error) {
	l.callStats.snapshot++
	return l.snapshot, nil
}

// Append writes the entries to persistent storage. Entries are then available
// through the Entries, Term, FirstIndex, and LastIndex methods.
func (l *DiskStorage) Append(entries []raftpb.Entry) error {
	l.callStats.append++
	if len(entries) == 0 {
		return nil
	}

	if l.deck.Count(TypeEntry) == 0 {
		l.firstEntry = raftpb.Entry{
			Term:  entries[0].Term,
			Index: entries[0].Index,
		}
	}

	var err error
	for _, entry := range entries {
		record := &storage.Record{Type: TypeEntry, Value: make([]byte, entry.Size())}
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

// SaveHardState writes the hard state to persistent storage,
// and makes it available for when Raft has to restart.
func (l *DiskStorage) SaveHardState(hardState raftpb.HardState) error {
	l.callStats.setHardState++

	record := &storage.Record{
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

// SaveSnapshot writes the snapshot to persistent storage,
// and makes it available through the Snapshot() method.
func (l *DiskStorage) SaveSnapshot(snapshot raftpb.Snapshot) error {
	l.callStats.applySnapshot++

	record := &storage.Record{
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

func (l *DiskStorage) compactionHandler() storage.CompactionHandler {
	var entry raftpb.Entry
	r := new(storage.Record)

	return func(c storage.Counters) error {
		for t, count := range c.All() {
			// We only do something with Entries atm.
			if t != TypeEntry {
				continue
			}

			err := l.deck.Get(TypeEntry, int(count)-1, r)
			if err != nil {
				return err
			}

			err = entry.Unmarshal(r.Value)
			if err != nil {
				return err
			}

			l.compactedEntry = raftpb.Entry{
				Index: entry.Index,
				Term:  entry.Term,
			}

			err = l.deck.Get(TypeEntry, int(count), r)
			if err != nil {
				return err
			}

			err = entry.Unmarshal(r.Value)
			if err != nil {
				return err
			}

			l.firstEntry = raftpb.Entry{
				Index: entry.Index,
				Term:  entry.Term,
			}
		}

		return nil
	}
}
