package raft

import (
	"bytes"
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

func NewDiskStorage(dir string, snap Snapshotter, c *storage.DeckConfig) (*DiskStorage, error) {
	s := &DiskStorage{
		deck: storage.NewDeck(dir, c),
		snap: snap,
	}

	s.deck.ReadAll()

	if s.deck.Count(TypeEntry) > 0 {
		value, err := s.deck.First(TypeEntry)
		if err != nil {
			return nil, err
		}

		var entry raftpb.Entry
		err = entry.Unmarshal(value)
		if err != nil {
			return nil, err
		}

		s.firstEntry = raftpb.Entry{
			Term:  entry.Term,
			Index: entry.Index,
		}
	}

	if s.deck.Count(TypeHardState) > 0 {
		value, err := s.deck.Last(TypeHardState)
		if err != nil {
			return nil, err
		}

		var hardState raftpb.HardState
		err = hardState.Unmarshal(value)
		if err != nil {
			return nil, err
		}

		s.hardState = hardState
	}

	if s.deck.Count(TypeSnapshot) > 0 {
		value, err := s.deck.Last(TypeSnapshot)
		if err != nil {
			return nil, err
		}

		var snapshot raftpb.Snapshot
		err = snapshot.Unmarshal(value)
		if err != nil {
			return nil, err
		}

		s.snapshot = snapshot
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

	// TODO: cutHandler is broken.
	s.deck.CutHandler(s.cutHandler())
	s.deck.CompactionHandler(s.compactionHandler())

	return s, nil
}

// TODO: Sync. And hope performance doesn't tank.
type DiskStorage struct {
	deck storage.Deck
	snap Snapshotter

	hardState raftpb.HardState
	snapshot  raftpb.Snapshot
	confState raftpb.ConfState

	appliedIndex   uint64
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

func (s *DiskStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.callStats.initialState++
	return s.hardState, s.snapshot.Metadata.ConfState, nil
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
func (s *DiskStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.callStats.entries++

	fi := s.firstIndex()
	if lo < fi {
		return nil, raft.ErrCompacted
	}

	lo -= fi
	hi -= fi

	var size uint64
	entries := make([]raftpb.Entry, 0)

	for value, err := range s.deck.Range(TypeEntry, int(lo), int(hi)) {
		if err != nil {
			return nil, fmt.Errorf("failed to get entries [lo: %d] [hi: %d]: %w", lo, hi, err)
		}

		size += uint64(len(value))
		if size > maxSize && len(entries) != 0 {
			return entries, nil
		}

		var entry raftpb.Entry
		err = entry.Unmarshal(value)
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
func (s *DiskStorage) Term(i uint64) (uint64, error) {
	s.callStats.term++
	if i == 0 && s.deck.Count(TypeEntry) == 0 {
		return 0, nil
	}

	li := s.lastIndex()
	if i > li {
		return 0, raft.ErrUnavailable
	}

	fi := s.firstIndex()
	if i == fi-1 {
		return s.compactedEntry.Term, nil
	}
	if i < fi || s.deck.Count(TypeEntry) == 0 {
		return 0, raft.ErrCompacted
	}

	i -= fi

	value, err := s.deck.Get(TypeEntry, int(i))
	if err != nil {
		return 0, fmt.Errorf("failed to get term [i: %d] [fi: %d] [li: %d]: %w", i, fi, li, err)
	}

	var entry raftpb.Entry
	err = entry.Unmarshal(value)
	if err != nil {
		return 0, err
	}

	return entry.Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (s *DiskStorage) LastIndex() (uint64, error) {
	s.callStats.lastIndex++
	return s.lastIndex(), nil
}

func (s *DiskStorage) lastIndex() uint64 {
	if s.deck.Count(TypeEntry) == 0 {
		return s.firstEntry.Index
	}
	return s.firstIndex() + uint64(s.deck.Count(TypeEntry)) - 1
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot).
func (s *DiskStorage) FirstIndex() (uint64, error) {
	s.callStats.firstIndex++
	return s.firstIndex(), nil
}

func (s *DiskStorage) AppliedIndex(i uint64) {
	s.appliedIndex = i
}

func (s *DiskStorage) firstIndex() uint64 {
	// Makes no sense, but here we are. This is how Raft's MemoryStorage works.
	// The Raft Node will refuse to start up without it.
	if s.deck.Count(TypeEntry) == 0 {
		return 1
	}
	return s.firstEntry.Index
}

// Snapshot returns the latest snapshot persisted to storage.
func (s *DiskStorage) Snapshot() (raftpb.Snapshot, error) {
	s.callStats.snapshot++
	return s.snapshot, nil
}

func (s *DiskStorage) Save(entries []raftpb.Entry, hardState raftpb.HardState, sync bool) error {
	if len(entries) == 0 && raft.IsEmptyHardState(hardState) {
		return nil
	}

	if len(entries) != 0 {
		if s.deck.Count(TypeEntry) == 0 {
			s.firstEntry = raftpb.Entry{
				Term:  entries[0].Term,
				Index: entries[0].Index,
			}
		}

		for _, entry := range entries {
			value, err := entry.Marshal()
			if err != nil {
				return err
			}

			err = s.deck.Append(TypeEntry, value)
			if err != nil {
				return err
			}
		}
	}

	if !raft.IsEmptyHardState(hardState) {
		value, err := hardState.Marshal()
		if err != nil {
			return err
		}

		err = s.deck.Append(TypeHardState, value)
		if err != nil {
			return err
		}

		s.hardState = hardState
	}

	if sync {
		return s.deck.Sync()
	}

	return nil
}

// SaveSnapshot writes the snapshot to persistent storage,
// and makes it available through the Snapshot() method.
func (s *DiskStorage) SaveSnapshot(snapshot raftpb.Snapshot) error {
	s.callStats.applySnapshot++

	value := make([]byte, snapshot.Size())
	_, err := snapshot.MarshalTo(value)
	if err != nil {
		return err
	}

	err = s.deck.Append(TypeSnapshot, value)
	if err != nil {
		return err
	}

	s.snapshot = snapshot

	return s.deck.Sync()
}

func (s *DiskStorage) SaveConfState(cs raftpb.ConfState) {
	s.confState = cs
}

func (s *DiskStorage) cutHandler() storage.CutHandler {
	var entry raftpb.Entry
	buf := new(bytes.Buffer)

	return func(seq int64) error {
		buf.Reset()

		value, err := s.deck.Get(TypeEntry, int(s.appliedIndex))
		if err != nil {
			return err
		}

		if err := entry.Unmarshal(value); err != nil {
			return err
		}

		if err := s.snap.Snapshot(buf); err != nil {
			return err
		}

		snap := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index:     entry.Index,
				Term:      entry.Term,
				ConfState: s.confState,
			},
			Data: buf.Bytes(),
		}

		data, err := snap.Marshal()
		if err != nil {
			return err
		}

		return s.deck.Append(TypeSnapshot, data)
	}
}

func (s *DiskStorage) compactionHandler() storage.CompactionHandler {
	var entry raftpb.Entry

	return func(c storage.Counters) error {
		for t, count := range c.All() {
			// We only do something with Entries atm.
			if t != TypeEntry {
				continue
			}

			value, err := s.deck.Get(TypeEntry, int(count)-1)
			if err != nil {
				return err
			}

			err = entry.Unmarshal(value)
			if err != nil {
				return err
			}

			s.compactedEntry = raftpb.Entry{
				Index: entry.Index,
				Term:  entry.Term,
			}

			value, err = s.deck.Get(TypeEntry, int(count))
			if err != nil {
				return err
			}

			err = entry.Unmarshal(value)
			if err != nil {
				return err
			}

			s.firstEntry = raftpb.Entry{
				Index: entry.Index,
				Term:  entry.Term,
			}
		}

		return nil
	}
}
