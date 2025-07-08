package storage

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	TypeEntry RecordType = iota
	TypeHardState
	TypeSnapshot
)

var (
	ErrUnknownEntryType = errors.New("found unknown entry type")
)

// EntryPointer stores the offset of an Entry within the log's file.
// It also stores the Term that the Entry belongs to as an optimization.
// The Term of an Entry is queried very often by Raft, and we can keep it in-memory
// for a negligble cost, saving disk reads and record decodes.
type EntryPointer struct {
	Offset int64
	Term   uint64
}

func NewLogStorage(r io.ReaderAt, w io.Writer) (*LogStorage, error) {
	l := &LogStorage{
		log: Log{
			enc: NewEncoder(w),
			dec: NewDecoder(r),
		},
		entries: make([]EntryPointer, 0),
	}

	var entry raftpb.Entry
	for record := range l.log.All() {
		switch record.Type {
		case TypeEntry:
			err := entry.Unmarshal(record.Value)
			if err != nil {
				return nil, err
			}

			l.entries = append(l.entries, EntryPointer{
				Offset: l.log.Pointer(),
				Term:   entry.Term,
			})

		case TypeHardState:
			err := l.hardState.Unmarshal(record.Value)
			if err != nil {
				return nil, err
			}

		case TypeSnapshot:
			err := l.snapshot.Unmarshal(record.Value)
			if err != nil {
				return nil, err
			}

		default:
			return nil, ErrUnknownEntryType
		}
	}

	if len(l.entries) != 0 {
		record := new(Record)
		err := l.log.ReadAt(record, l.entries[0].Offset)
		if err != nil {
			return nil, err
		}

		err = entry.Unmarshal(record.Value)
		if err != nil {
			return nil, err
		}

		l.indexOffset = entry.Index
	}

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

	return l, nil
}

type LogStorage struct {
	log Log

	hardState raftpb.HardState
	snapshot  raftpb.Snapshot

	// entries stores a series of pointers to Entries in the log file.
	// It is assumed that the Index of every Entry is sequential and without gaps.
	// It also stores each Entry's Term as an optimization.
	entries []EntryPointer
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
	var err error
	entries := make([]raftpb.Entry, 0)
	record := new(Record)

	for _, ptr := range l.entries[lo:hi] {
		err = l.log.ReadAt(record, ptr.Offset)
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
func (l *LogStorage) LastIndex() (uint64, error) {
	l.callStats.lastIndex++
	return l.lastIndex(), nil
}

func (l *LogStorage) lastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.indexOffset
	}
	return l.firstIndex() + uint64(len(l.entries)) - 1
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
	if len(l.entries) == 0 {
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

	err = l.log.Append(record)
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

	err = l.log.Append(record)
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

	if len(l.entries) == 0 {
		l.indexOffset = entries[0].Index
	}

	var err error
	for _, entry := range entries {
		record := &Record{Type: TypeEntry, Value: make([]byte, entry.Size())}
		_, err = entry.MarshalTo(record.Value)
		if err != nil {
			return err
		}

		err = l.log.Append(record)
		if err != nil {
			return err
		}

		l.entries = append(l.entries, EntryPointer{
			Offset: l.log.Pointer(),
			Term:   entry.Term,
		})
	}

	return nil
}

func (l *LogStorage) Compact(i uint64) error {
	l.callStats.compact++
	return nil
}

type DeckConfig struct {
	MaxLogSize  int64
	MaxLogCount int
}

var defaultDeckConfig = DeckConfig{
	MaxLogSize:  64 << 20,
	MaxLogCount: 16,
}

func NewDeck(dir string, c DeckConfig) Deck {
	d := Deck{
		logs: make([]*Log, 0),

		dir:         dir,
		maxLogSize:  defaultDeckConfig.MaxLogSize,
		maxLogCount: defaultDeckConfig.MaxLogCount,

		compactions: make(chan int64),
	}

	if c.MaxLogSize != 0 {
		d.maxLogSize = c.MaxLogSize
	}

	if c.MaxLogCount != 0 {
		d.maxLogCount = c.MaxLogCount
	}

	runtime.SetFinalizer(d, func() {
		close(d.compactions)
	})

	return d
}

// Deck is an organized collection of Logs.
type Deck struct {
	logs []*Log
	// dir is the directory where the Logs are stored on-disk.
	dir string
	// sequence holds the ID of the last log created.
	sequence uint64
	// offset translates on-disk Log indices to their in-memory equivalent.
	offset int64
	// activeLogSize is the current size of the Log that is being appended to.
	// This can be fetched from the Log's cursor, so will probably remove.
	activeLogSize int64
	// maxLogSize determines the maximum size that a log can have.
	// If a Record will cause a Log to exceed its maximum allowed size,
	// a new Log will be provisioned and the Record will be appended there.
	maxLogSize int64
	// maxLogCount determines the maximum amount of logs stored in the Deck.
	// When the number of logs exceeds this number, the oldest log will be compacted.
	maxLogCount int
	// compactions transmits IDs of Logs that have been compacted by the Deck.
	// The client app is expected to read this channel and take actions based on it.
	compactions chan int64
}

func (d *Deck) Append(r *Record) error {
	log := d.activeLog()
	if log.cursor+r.Size() > d.maxLogSize {
		log = d.newLog()
	}

	return log.Append(r)
}

func (d *Deck) ReadAt(r *Record, p Pointer) error {
	return d.logs[p.Log-d.offset].ReadAt(r, p.Offset)
}

func (d *Deck) All() iter.Seq[*Log] {
	return func(yield func(*Log) bool) {
		for _, log := range d.logs {
			if !yield(log) {
				return
			}
		}
	}
}

func (d *Deck) newLog() *Log {
	defer d.compact()

	logFile := filepath.Join(d.dir, fmt.Sprintf("%20d.log", d.sequence))

	w, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	r, err := os.OpenFile(logFile, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}

	log := NewLog(r, w)
	d.logs = append(d.logs, log)
	d.sequence++

	return log
}

func (d *Deck) activeLog() *Log {
	if len(d.logs) == 0 {
		return d.newLog()
	}
	return d.logs[len(d.logs)-1]
}

func (d *Deck) compact() {
	if len(d.logs) > d.maxLogCount {
		id := d.logs[0].ID
		logFile := filepath.Join(d.dir, fmt.Sprintf("%20d.log", id))
		err := os.Remove(logFile)
		if err != nil {
			panic(err)
		}
		d.logs = d.logs[1 : len(d.logs)-1]
		d.compactions <- id
	}
}

// Compactions transmits IDs of Logs that have been compacted by the Deck.
func (d *Deck) Compactions() <-chan int64 {
	return d.compactions
}

// Pointer returns the location of the Record last written to the Deck.
func (d *Deck) Pointer() Pointer {
	return Pointer{
		Log:    d.logs[len(d.logs)-1].ID,
		Offset: d.logs[len(d.logs)-1].Pointer(),
	}
}

type Pointer struct {
	Log    int64
	Offset int64
}

func NewLog(r io.ReaderAt, w io.Writer) *Log {
	return &Log{
		enc: NewEncoder(w),
		dec: NewDecoder(r),
	}
}

type Log struct {
	ID int64

	enc Encoder
	dec Decoder
	// cursor tracks the position of writes to the log.
	// The log is only ever appended to, and cursor only ever increases.
	cursor int64
	// pointer contains the starting position of the last record
	// written to the log.
	pointer int64
}

func (l *Log) Append(r *Record) error {
	n, err := l.enc.Encode(r)
	l.advance(n)
	return err
}

func (l *Log) ReadAt(r *Record, offset int64) error {
	_, err := l.dec.DecodeAt(r, offset)
	return err
}

func (l *Log) All() iter.Seq[*Record] {
	return func(yield func(*Record) bool) {
		r := new(Record)
		for {
			n, err := l.dec.DecodeAt(r, l.cursor)
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Panicln("error while reading log:", err)
			}
			l.advance(n)

			if !yield(r) {
				break
			}
		}
	}
}

func (l *Log) Pointer() int64 {
	return l.pointer
}

func (l *Log) Rewind() {
	l.cursor = 0
	l.pointer = 0
}

func (l *Log) advance(n int64) {
	l.pointer = l.cursor
	l.cursor += n
}
