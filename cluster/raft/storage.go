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

	confState    raftpb.ConfState // TODO: Remove, should only be persisted on disk.
	appliedIndex uint64           // TODO: Probably also remove.

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

	if s.db.Count(TypeSnapshot) > 0 {
		data := make([]byte, 0)
		data, err = s.db.Last(TypeSnapshot)
		if err != nil {
			return
		}

		var snapshot raftpb.Snapshot
		err = snapshot.Unmarshal(data)
		if err != nil {
			return
		}

		cs = snapshot.Metadata.ConfState
	}

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

	for value, err := range s.db.Range(TypeEntry, int(lo), int(hi)) {
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
	if i == 0 && s.db.Count(TypeEntry) == 0 {
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
	// Makes no sense, but here we are. This is how Raft's MemoryStorage works.
	// The Raft Node will refuse to start up without it.
	if s.db.Count(TypeEntry) == 0 {
		return 1
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

// TODO: Probably remove
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

func (s *DiskStorage) SaveAppliedIndex(i uint64) error {
	s.appliedIndex = i
	return nil
}

func (s *DiskStorage) CreateSnapshot() error {
	buf := new(bytes.Buffer)
	err := s.snap.Snapshot(buf)
	if err != nil {
		return err
	}

	snapshot := raftpb.Snapshot{
		Data: buf.Bytes(),
		Metadata: raftpb.SnapshotMetadata{
			Index:     s.appliedIndex,
			Term:      s.terms[s.appliedIndex-s.firstIndex()],
			ConfState: s.confState,
		},
	}

	data, err := snapshot.Marshal()
	if err != nil {
		return err
	}

	return s.db.Append(TypeSnapshot, data)
}

// TODO: ApplySnapshot does not actually apply the snapshot. See Iotas notes on Snapshotting.
func (s *DiskStorage) ApplySnapshot(snapshot raftpb.Snapshot) error {
	value := make([]byte, snapshot.Size())
	_, err := snapshot.MarshalTo(value)
	if err != nil {
		return err
	}

	err = s.db.Append(TypeSnapshot, value)
	if err != nil {
		return err
	}

	return s.db.Sync()
}

func (s *DiskStorage) SaveConfState(cs raftpb.ConfState) {
	s.confState = cs
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
	var entry raftpb.Entry
	buf := new(bytes.Buffer)

	// TODO: Basically this is what CreateSnapshot would be. Except now it's really hard to test.
	// It should be split off into a CreateSnapshot method so that it's easier to test.
	// And then this cuthook would just call that method. Probably with a conditional so that
	// a snapshot is not made for every single log cut.
	return func(id qwal.LogID) error {
		buf.Reset()

		value, err := s.db.Get(TypeEntry, int(s.appliedIndex-s.firstIndex()))
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

		err = s.db.Append(TypeSnapshot, data)
		if err != nil {
			return err
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

			value, err := s.db.Get(TypeEntry, int(count-1))
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

			value, err = s.db.Get(TypeEntry, int(count))
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
