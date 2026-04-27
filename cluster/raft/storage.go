package raft

import (
	"bytes"
	"fmt"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// Types of Records being saved to storage.
	TypeEntry qwal.Type = iota
	TypeHardState
	TypeSnapshot
)

func NewDiskStorage(db qwal.DB, snap cluster.Snapshotter) (*DiskStorage, error) {
	s := &DiskStorage{
		db:   db,
		snap: snap,
	}

	s.db.ReplayHook(s.replayHook())
	s.db.CutHook(s.cutHook())
	s.db.CompactHook(s.compactionHook())

	if err := db.Replay(); err != nil {
		return nil, err
	}

	if s.db.Count(TypeSnapshot) > 0 {
		value, err := s.db.Last(TypeSnapshot)
		if err != nil {
			return nil, fmt.Errorf("failed to read snapshot: %w", err)
		}

		var snap raftpb.Snapshot
		err = snap.Unmarshal(value)
		if err != nil {
			return nil, err
		}

		s.appliedIndex = snap.Metadata.Index
		s.confState = snap.GetMetadata().ConfState
	}

	if s.db.Count(TypeEntry) > 0 {
		value, err := s.db.First(TypeEntry)
		if err != nil {
			return nil, fmt.Errorf("failed to read first entry: %w", err)
		}

		var entry raftpb.Entry
		err = entry.Unmarshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal first entry: %w", err)
		}

		s.firstEntry = raftpb.Entry{
			Term:  entry.Term,
			Index: entry.Index,
		}
	}

	return s, nil
}

// DiskStorage implements a disk-backed implementation of [raft.Storage].
type DiskStorage struct {
	// db serves as the backing storage for this DiskStorage.
	db qwal.DB
	// snap is used for generating snapshots. Typically supplied
	// by the application.
	snap cluster.Snapshotter
	// terms is a cache for the Terms of Entries. These are queried very often
	// by the Raft state machine by the index of the Entry that they belong to.
	terms []uint64

	// confState is set by the node, and persisted in snapshots.
	confState raftpb.ConfState
	// appliedIndex is set by the node, and persisted in snapshots.
	appliedIndex uint64

	// firstEntry is the oldest Entry available in the storage.
	firstEntry raftpb.Entry
	// compactedEntry is the last Entry that was removed by compaction. It is preserved for
	// consistency checking, because the first Entry in the storage needs a previous log index and term.
	compactedEntry raftpb.Entry
}

// InitialState implements [raft.Storage.InitialState].
func (s *DiskStorage) InitialState() (hs raftpb.HardState, cs raftpb.ConfState, err error) {
	if s.db.Count(TypeHardState) > 0 {
		data := make([]byte, hs.Size())
		data, err = s.db.Last(TypeHardState)
		if err != nil {
			return
		}

		err = hs.Unmarshal(data)
		if err != nil {
			return
		}
	}

	cs = s.confState
	return
}

// Entries implements [raft.Storage.Entries].
func (s *DiskStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	fi := s.firstIndex()
	if lo < fi {
		return nil, raft.ErrCompacted
	}

	lo -= fi
	hi -= fi

	var size uint64
	entries := make([]raftpb.Entry, 0)

	for value, err := range s.db.Range(TypeEntry, lo, hi) {
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

// Term implements [raft.Storage.Term].
func (s *DiskStorage) Term(i uint64) (uint64, error) {
	if i == s.compactedEntry.Index {
		return s.compactedEntry.Term, nil
	}

	if i == 0 && s.db.Count(TypeEntry) == 0 {
		return 0, nil
	}

	li := s.lastIndex()
	if i > li {
		return 0, raft.ErrUnavailable
	}

	fi := s.firstIndex()
	if i < fi || s.db.Count(TypeEntry) == 0 {
		return 0, raft.ErrCompacted
	}

	i -= fi

	return s.terms[i], nil
}

// LastIndex implements [raft.Storage.LastIndex].
func (s *DiskStorage) LastIndex() (uint64, error) {
	return s.lastIndex(), nil
}

func (s *DiskStorage) lastIndex() uint64 {
	if s.db.Count(TypeEntry) == 0 {
		return s.firstEntry.Index
	}
	return s.firstIndex() + uint64(s.db.Count(TypeEntry)) - 1
}

// FirstIndex implements [raft.Storage.FirstIndex].
func (s *DiskStorage) FirstIndex() (uint64, error) {
	return s.firstIndex(), nil
}

func (s *DiskStorage) firstIndex() uint64 {
	if s.db.Count(TypeEntry) == 0 {
		return s.compactedEntry.Index + 1
	}
	return s.firstEntry.Index
}

// Snapshot implements [raft.Storage.Snapshot].
func (s *DiskStorage) Snapshot() (raftpb.Snapshot, error) {
	if s.db.Count(TypeSnapshot) == 0 {
		return raftpb.Snapshot{}, nil
	}

	var snap raftpb.Snapshot
	data, err := s.db.Last(TypeSnapshot)
	if err != nil {
		return snap, err
	}

	err = snap.Unmarshal(data)
	return snap, err
}

func (s *DiskStorage) AppliedIndex() uint64 {
	return s.appliedIndex
}

func (s *DiskStorage) Save(entries []raftpb.Entry, hardState raftpb.HardState, sync bool) error {
	if len(entries) != 0 {
		if s.db.Count(TypeEntry) == 0 {
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

			err = s.db.Append(TypeEntry, value)
			if err != nil {
				return err
			}

			s.terms = append(s.terms, entry.Term)
		}
	}

	if !raft.IsEmptyHardState(hardState) {
		value, err := hardState.Marshal()
		if err != nil {
			return err
		}

		err = s.db.Append(TypeHardState, value)
		if err != nil {
			return err
		}
	}

	if sync {
		return s.db.Sync()
	}

	return nil
}

// SetConfState stores the given ConfState in-memory.
// It will be persisted to stable storage with the next snapshot.
func (s *DiskStorage) SetConfState(cs raftpb.ConfState) {
	s.confState = cs
}

// SetAppliedIndex stores the given index in-memory. The applied index indicates to which Raft entry
// has been applied to the application's state machine. It will be persisted to stable storage
// with the next snapshot.
func (s *DiskStorage) SetAppliedIndex(i uint64) {
	s.appliedIndex = i
}

func (s *DiskStorage) CreateSnapshot() error {
	buf := new(bytes.Buffer)
	err := s.snap.Snapshot(buf)
	if err != nil {
		return err
	}

	var term uint64 = 1
	if len(s.terms) > 0 && s.appliedIndex > 0 {
		term = s.terms[s.appliedIndex-s.firstIndex()]
	}

	snapshot := raftpb.Snapshot{
		Data: buf.Bytes(),
		Metadata: raftpb.SnapshotMetadata{
			Index:     s.appliedIndex,
			Term:      term,
			ConfState: s.confState,
		},
	}

	data, err := snapshot.Marshal()
	if err != nil {
		return err
	}

	return s.db.Append(TypeSnapshot, data)
}

func (s *DiskStorage) ApplySnapshot(snapshot raftpb.Snapshot) error {
	// Clear the cut hook so that no automatic snapshots are created.
	s.db.CutHook(nil)
	defer s.db.CutHook(s.cutHook())

	// Cut a fresh log to write our snapshot into.
	if err := s.db.Cut(); err != nil {
		return err
	}

	value := make([]byte, snapshot.Size())
	if _, err := snapshot.MarshalTo(value); err != nil {
		return err
	}

	if err := s.db.Append(TypeSnapshot, value); err != nil {
		return err
	}

	// Discard all older logs.
	if err := s.db.Discard(); err != nil {
		return err
	}

	s.confState = snapshot.GetMetadata().ConfState
	s.compactedEntry = raftpb.Entry{
		Index: snapshot.Metadata.Index,
		Term:  snapshot.Metadata.Term,
	}
	s.terms = []uint64{snapshot.Metadata.Term}

	return s.db.Sync()
}

func (s *DiskStorage) Sync() error {
	return s.db.Sync()
}

func (s *DiskStorage) Close() error {
	return s.db.Close()
}

func (s *DiskStorage) replayHook() qwal.ReplayHookFunc {
	var entry raftpb.Entry

	return func(t qwal.Type, v []byte) error {
		if t != TypeEntry {
			return nil
		}
		if err := entry.Unmarshal(v); err != nil {
			return err
		}
		s.terms = append(s.terms, entry.Term)
		return nil
	}
}

func (s *DiskStorage) cutHook() qwal.CutHookFunc {
	desiredSnapshots := 3
	return func(id qwal.LogID) error {
		maxLogs := s.db.Status().MaxLogCount
		if int(id)%(maxLogs/desiredSnapshots) == 0 {
			return s.CreateSnapshot()
		}
		return nil
	}
}

func (s *DiskStorage) compactionHook() qwal.CompactHookFunc {
	var entry raftpb.Entry

	return func(c qwal.Counters) error {
		for t, count := range c {
			// We only do something with Entries atm.
			if t != TypeEntry {
				continue
			}

			value, err := s.db.Get(TypeEntry, count-1)
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

			value, err = s.db.Get(TypeEntry, count)
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

			s.terms = s.terms[count:]
		}

		return nil
	}
}
