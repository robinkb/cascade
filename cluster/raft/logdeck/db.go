package logdeck

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"syscall"

	"golang.org/x/exp/mmap"
)

type (
	// DB represents a sequential collection of values, organized into Logs,
	// and indexed by type and sequence. New values are appended to the active Log.
	// Once the active Log reaches MaxLogSize, a new Log is provisioned to
	// receive new appends. The old Log becomes read-only. Once DB exceeds
	// MaxLogCount, compaction removes the oldest Log and its values.
	DB interface {
		// Append writes a value to the DB.
		Append(t Type, value []byte) error
		// Get retrieves a value with Type t at index i.
		Get(t Type, i int) ([]byte, error)
		// Count returns how many values with Type t are in DB.
		Count(t Type) int
		// First returns the first value of Type t in the DB. The value returned
		// by this method changes after a compaction.
		First(t Type) ([]byte, error)
		// Last returns the last value of Type t that was written to the DB.
		Last(t Type) ([]byte, error)
		// Range returns an iterator that ranges over all values of Type t
		// in the range [lo, hi[.
		Range(t Type, lo, hi int) iter.Seq2[[]byte, error]
		// Cut manually cuts a new Log in the DB. Cutting a Log is normally
		// triggered automatically when MaxLogSize or MaxLogRecordCount is
		// exceeded. Instead, Cut may be used to trigger them manually when more
		// control is required, like for tests. As such, Cut does not consider
		// MaxLogSize and MaxLogRecordCount.
		Cut() error
		// Compact manually triggers a compaction. Compactions are normally
		// triggered automatically when MaxLogCount is exceeded. Instead,
		// Compact may be used to trigger them manually when more control
		// is required, like for tests. As such, Compact does not consider MaxLogCount.
		// Attempting to Compact when the DB only contains one Log returns ErrInvalidCompaction.
		Compact() error
		// CutHook registers CutHookFunc f, which is run whenever a Log is cut.
		// To clear the CutHook, call CutHook with a nil argument.
		CutHook(f CutHookFunc)
		// CompactHook registers CompactHook f, which is run whenever DB compacts a Log.
		// To clear the CompactHook, call CompactHook with a nil argument.
		CompactHook(f CompactHookFunc)
		ReadAll()
		// Sync calls syscall.Fdatasync on the active Log, ensuring that buffered
		// writes to it are flushed to disk. DB only syncs automatically when a Log
		// is cut and becomes read-noly. Any more syncs are the application's responsibility.
		Sync() error
		// Close closes the DB, flushing any pending writes to disk.
		Close() error
	}

	// Type represents the type of a value appended to the DB. Consumers of DB
	// are expected to define their own types as constants of Type.
	Type uint32

	// LogID represents the sequential ID of a Log in the DB.
	LogID uint64

	// CutHookFunc is executed whenever a Log is cut. A Log is cut when it reaches
	// its maximum size and is moved into read-only mode. A new Log is then
	// provisioned to receive new writes. CutHookFunc is executed right after
	// the new Log is provisioned. If Append is called on the LogDeck in
	// CutHookFunc, the appended value is guaranteed to be the first in the new Log.
	CutHookFunc func(id LogID) error

	// CompactHookFunc is executed whenever a Log is compacted. Compaction is
	// triggered when a newly provisioned Log causes MaxLogCount to be exceeded.
	// CompactionHookFunc is executed right before the oldest Log is actually
	// removed from DB, meaning that its data can still be queried for the
	// duration of CompactHookFunc.
	// TODO: Instead of Counters, maybe have it be an iter.Seq2[Type, uint64],
	// the signature of Counters.All(). That way we can make Counters private.
	CompactHookFunc func(c Counters) error

	// Options defines the configurable options of the DB.
	Options struct {
		// MaxLogSize determines the maximum size that a single Log in the DB can have.
		// When appending a value to a Log would make it grow larger than MaxLogSize,
		// a new Log is provisioned, and the value is appended to the new Log.
		// The total maximum DB size on disk is MaxLogSize * (MaxLogCount + 1).
		MaxLogSize int64
		// MaxLogValueCount determines the maximum amount of values that a single Log in the DB can have.
		// When appending a value to a Log would make it exceed MaxLogValueCount,
		// a new Log is provisioned, and the value is appended to the new Log.
		MaxLogValueCount int64
		// MaxLogCount determines how many Logs can be contained in the DB.
		// Once exceeded, the oldest Log in the DB is compacted.
		// The total maximum DB size on disk is MaxLogSize * (MaxLogCount + 1).
		MaxLogCount int
	}
)

// DefaultOptions defines the default Options values.
var DefaultOptions = &Options{
	MaxLogSize:       64 << 20,
	MaxLogValueCount: 10000,
	MaxLogCount:      16,
}

// Open intializes a DB in directory dir. If the directory already contains
// Logs belonging to a DB, the DB reads the Logs to restore its in-memory state.
func Open(dir string, opts *Options) (DB, error) {
	db := db{
		logs: make([]*logFile, 0),

		dir: dir,

		maxLogSize:        DefaultOptions.MaxLogSize,
		maxLogRecordCount: DefaultOptions.MaxLogValueCount,
		maxLogCount:       DefaultOptions.MaxLogCount,

		inventory: newInventory(),
	}

	if opts != nil {
		if opts.MaxLogSize != 0 {
			db.maxLogSize = opts.MaxLogSize
		}

		if opts.MaxLogValueCount != 0 {
			db.maxLogRecordCount = opts.MaxLogValueCount
		}

		if opts.MaxLogCount != 0 {
			db.maxLogCount = opts.MaxLogCount
		}
	}

	info, err := os.Stat(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(db.dir, 0755)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		// TODO: Write test for this case.
		if !info.IsDir() {
			return nil, errors.New("not a directory")
		}
	}

	logFiles, err := discoverLogFiles(dir)
	if err != nil {
		return nil, err
	}

	for _, name := range logFiles {
		id, err := strconv.ParseUint(name[:len(name)-4], 10, 64)
		if err != nil {
			return nil, err
		}

		name := filepath.Join(db.dir, name)
		w, err := os.OpenFile(name, os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}

		r, err := mmap.Open(name)
		if err != nil {
			return nil, err
		}

		db.logs = append(db.logs, &logFile{
			ID:   LogID(id),
			File: w,
			log:  newLog(r, w),
		})
		db.sequence++
	}

	if len(db.logs) == 0 {
		_, err := db.newLog()
		if err != nil {
			return nil, err
		}
	}

	return &db, nil
}

// db implements the DB interface.
type db struct {
	logs []*logFile
	// dir is the directory where the Logs are stored on-disk.
	dir string
	// sequence holds the ID of the last log created.
	sequence uint64
	// offset translates on-disk Log indices to their in-memory equivalent.
	offset uint64
	// maxLogSize determines the maximum size that a Log can have.
	// If a Record will cause a Log to exceed its maximum allowed size,
	// a new Log will be provisioned and the Record will be appended there.
	maxLogSize int64
	// maxLogRecordCount determines the maximum amount of records that a Log can have.
	// If a Record will cause a Log to exceed its maximum allowed size,
	// a new Log will be provisioned and the Record will be appended there.
	maxLogRecordCount int64
	// maxLogCount determines the maximum amount of logs stored in the Deck.
	// When the number of logs exceeds this number, the oldest log will be compacted.
	maxLogCount int
	// Inventory holds pointers to all known Records in the Deck,
	// organized by type. It grows when appending Records to the Deck,
	// and shrinks when Logs in the Deck are compacted.
	inventory *inventory
	// cutHook is provided by the Deck consumer. If provided,
	// it is called by Deck after every Log is cut.
	cutHook CutHookFunc
	// compactHook is provided by the Deck consumer. If provided,
	// it is called by Deck after every compaction, allowing the consumer
	// to update its internal bookkeeping.
	compactHook CompactHookFunc
}

func (d *db) Append(t Type, value []byte) error {
	var err error
	r := &record{
		Type:  t,
		Value: value,
	}

	log := d.activeLog()
	if d.appendWouldExceedLimits(log, r) {
		log, err = d.cut()
		if err != nil {
			return err
		}
	}

	err = log.Append(r)
	if err != nil {
		return err
	}

	d.inventory.Add(r.Type, d.pointer())

	// Run compaction _after_ adding the new entry so that
	// the compaction handler has an up-to-date view of the Deck.
	if len(d.logs) > d.maxLogCount {
		return d.compact()
	}

	return nil
}

func (d *db) appendWouldExceedLimits(log *logFile, r *record) bool {
	return d.maxLogSize < log.cursor+r.size() ||
		uint64(d.maxLogRecordCount) <= log.counters.total()
}

func (d *db) Sync() error {
	return d.activeLog().Sync()
}

func (d *db) Get(t Type, i int) ([]byte, error) {
	ptr, err := d.inventory.Get(t, i)
	if err != nil {
		return nil, fmt.Errorf("could not get pointer to record: %w", err)
	}

	return d.valueAt(ptr)
}

func (d *db) Count(t Type) int {
	return d.inventory.Count(t)
}

func (d *db) First(t Type) ([]byte, error) {
	ptr, err := d.inventory.Get(t, 0)
	if err != nil {
		return nil, err
	}

	return d.valueAt(ptr)
}

func (d *db) Last(t Type) ([]byte, error) {
	ptr, err := d.inventory.Get(t, d.inventory.Count(t)-1)
	if err != nil {
		return nil, err
	}

	return d.valueAt(ptr)
}

func (d *db) Range(t Type, lo, hi int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		pointers, err := d.inventory.Range(t, lo, hi)
		if err != nil {
			yield(nil, err)
			return
		}

		for _, ptr := range pointers {
			b, err := d.valueAt(ptr)
			if !yield(b, err) {
				return
			}
		}
	}
}

func (d *db) valueAt(p pointer) ([]byte, error) {
	value := make([]byte, p.Size)
	err := d.logs[uint64(p.Log)-d.offset].ValueAt(value, p.Offset)
	return value, err
}

func (d *db) ReadAll() {
	for _, log := range d.logs {
		for record := range log.All() {
			offset, size := log.Pointer()

			d.inventory.Add(record.Type, pointer{
				Log:    log.ID,
				Offset: offset,
				Size:   size,
			})
		}
	}
}

func (d *db) Cut() error {
	_, err := d.cut()
	return err
}

func (d *db) CutHook(h CutHookFunc) {
	d.cutHook = h
}

func (d *db) Compact() error {
	return d.compact()
}

func (d *db) CompactHook(h CompactHookFunc) {
	d.compactHook = h
}

func (d *db) Close() error {
	// TODO: Should also close other logs.
	return d.activeLog().Close()
}

func (d *db) activeLog() *logFile {
	return d.logs[len(d.logs)-1]
}

func (d *db) newLog() (*logFile, error) {
	filename := filepath.Join(d.dir, fmt.Sprintf("%020d.log", d.sequence))

	w, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	err = syscall.Fallocate(int(w.Fd()), 0, 0, d.maxLogSize)
	if err != nil {
		return nil, err
	}

	r, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}

	log := &logFile{
		log:  newLog(r, w),
		ID:   LogID(d.sequence),
		File: w,
		Mmap: r,
	}
	d.logs = append(d.logs, log)

	d.sequence++

	return log, nil
}

func (d *db) cut() (*logFile, error) {
	oldLog := d.activeLog()
	err := oldLog.Sync()
	if err != nil {
		return nil, err
	}

	newLog, err := d.newLog()
	if err != nil {
		return nil, err
	}

	if d.cutHook != nil {
		err := d.cutHook(oldLog.ID)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrCutHookFailed, err)
		}
	}

	return newLog, nil
}

func (d *db) compact() error {
	if len(d.logs) <= 1 {
		return fmt.Errorf("%w: only one Log in DB", ErrInvalidCompaction)
	}

	log := d.logs[0]

	// The CompactHook is run _before_ compaction actually occurs
	// so that the Deck consumer has a full view of the data, including
	// what is being removed. An errors encountered by compaction are surfaced
	// to the application.
	if d.compactHook != nil {
		err := d.compactHook(log.Counters())
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCompactHookFailed, err)
		}
	}

	if err := log.Close(); err != nil {
		return err
	}

	if err := os.Remove(log.File.Name()); err != nil {
		return err
	}

	d.inventory.Remove(log.Counters())

	d.logs = d.logs[1:len(d.logs)]
	d.offset++

	return nil
}

// pointer returns the location of the Record last written to the DB.
func (d *db) pointer() pointer {
	log := d.activeLog()
	offset, size := log.Pointer()

	return pointer{
		Log:    log.ID,
		Offset: offset,
		Size:   size,
	}
}

var logNameRe = regexp.MustCompile(`(\d{20}).log`)

// logFile represents a Log that is backed by a file.
// It wraps the basic Log and adds an ID for tracking unique Logs,
// and the handles of the underlying file.
type logFile struct {
	*log
	ID   LogID
	File *os.File
	Mmap *mmap.ReaderAt
}

func (l *logFile) Sync() error {
	return syscall.Fdatasync(int(l.File.Fd()))
}

func (l *logFile) Close() error {
	err := l.Sync()
	if err != nil {
		return err
	}

	err = l.File.Close()
	if err != nil {
		return err
	}

	return l.Mmap.Close()
}

func discoverLogFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	logFiles := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		if logNameRe.MatchString(e.Name()) {
			logFiles = append(logFiles, e.Name())
		}
	}

	slices.Sort(logFiles)
	return logFiles, nil
}
