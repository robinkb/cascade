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
	// DB represents a collection of values, indexed by type and sequence.
	DB interface {
		Append(t Type, value []byte) error
		Sync() error
		Get(t Type, i int) ([]byte, error)
		Count(t Type) int
		First(t Type) ([]byte, error)
		Last(t Type) ([]byte, error)
		Range(t Type, lo, hi int) iter.Seq2[[]byte, error]
		ReadAll()
		CutHook(f CutHookFunc)
		CompactHook(f CompactHookFunc)
	}

	// CutHookFunc is executed whenever a Log is cut. A Log is cut when it reaches
	//its maximum size and is moved into read-only mode. A new Log is then
	// provisioned to receive new writes. CutHookFunc is executed right after
	// the new Log is provisioned. If Append is called on the LogDeck in
	// CutHookFunc, it is guaranteed to be the first record in the new Log.
	CutHookFunc func(seq int64) error

	// CompactHookFunc is executed whenever a Log is compacted. Compaction is
	// triggered when a newly provisioned Log causes MaxLogCount to be exceeded.
	// CompactionHookFunc is executed right before the oldest Log is actually
	// removed from LogDeck, meaning that its data can still be queried for the
	// duration of CompactHookFunc.
	// TODO: Instead of Counters, maybe have it be an iter.Seq2[Type, uint64],
	// the signature of Counters.All(). That way we can make Counters private.
	CompactHookFunc func(c Counters) error

	Options struct {
		// MaxLogSize determines the maximum size that a single Log in the LogDeck
		// can have. When appending a value to a Log would make it grow larger
		// than MaxLogSize, a new Log is provisioned, and the value is appended
		// there. The total maximum Deck size on disk is MaxLogSize * MaxLogCount.
		MaxLogSize int64
		// MaxLogCount determines how many Logs can be contained in the LogDeck.
		// Once exceeded, the oldest Log in the Deck is compacted.
		// The total maximum Deck size on disk is MaxLogSize * MaxLogCount.
		MaxLogCount int
	}
)

var DefaultOptions = &Options{
	MaxLogSize:  64 << 20,
	MaxLogCount: 16,
}

func Open(dir string, opts *Options) (DB, error) {
	db := db{
		logs: make([]managedLog, 0),

		dir:         dir,
		maxLogSize:  DefaultOptions.MaxLogSize,
		maxLogCount: DefaultOptions.MaxLogCount,

		inventory: newInventory(),
	}

	if opts != nil {
		if opts.MaxLogSize != 0 {
			db.maxLogSize = opts.MaxLogSize
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
				panic(err)
			}
		} else {
			panic(err)
		}
	} else {
		// TODO: Write test for this case.
		if !info.IsDir() {
			panic("not a directory")
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
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

	for _, name := range logFiles {
		id, err := strconv.ParseInt(name[:len(name)-4], 10, 64)
		if err != nil {
			panic(err)
		}

		name := filepath.Join(db.dir, name)
		w, err := os.OpenFile(name, os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		r, err := mmap.Open(name)
		if err != nil {
			panic(err)
		}

		db.logs = append(db.logs, managedLog{
			ID:   id,
			File: w,
			log:  newLog(r, w),
		})
		db.sequence++
	}

	return &db, nil
}

// db implements the DB interface.
type db struct {
	logs []managedLog
	// dir is the directory where the Logs are stored on-disk.
	dir string
	// sequence holds the ID of the last log created.
	sequence uint64
	// offset translates on-disk Log indices to their in-memory equivalent.
	offset int64
	// maxLogSize determines the maximum size that a log can have.
	// If a Record will cause a Log to exceed its maximum allowed size,
	// a new Log will be provisioned and the Record will be appended there.
	maxLogSize int64
	// maxLogCount determines the maximum amount of logs stored in the Deck.
	// When the number of logs exceeds this number, the oldest log will be compacted.
	maxLogCount int
	// Inventory holds pointers to all known Records in the Deck,
	// organized by type. It grows when appending Records to the Deck,
	// and shrinks when Logs in the Deck are compacted.
	inventory *inventory
	// cutHandler is provided by the Deck consumer. If provided,
	// it is called by Deck after every Log is cut.
	cutHandler CutHookFunc
	// compactHandler is provided by the Deck consumer. If provided,
	// it is called by Deck after every compaction, allowing the consumer
	// to update its internal bookkeeping.
	compactHandler CompactHookFunc
}

func (d *db) Append(t Type, value []byte) error {
	r := &record{
		Type:  t,
		Value: value,
	}

	log := d.activeLog()
	if log.cursor+r.size() > d.maxLogSize {
		log = d.newLog()
	}

	err := log.Append(r)
	if err != nil {
		return err
	}

	d.inventory.Add(r.Type, d.pointer())

	// Run compaction _after_ adding the new entry so that
	// the compaction handler has an up-to-date view of the Deck.
	if len(d.logs) > d.maxLogCount {
		d.compact()
	}

	return nil
}

func (d *db) Sync() error {
	return syscall.Fdatasync(int(d.activeLog().File.Fd()))
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
	err := d.logs[p.Log-d.offset].ValueAt(value, p.Offset)
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

func (d *db) CutHook(h CutHookFunc) {
	d.cutHandler = h
}

func (d *db) CompactHook(h CompactHookFunc) {
	d.compactHandler = h
}

func (d *db) newLog() managedLog {
	logFile := filepath.Join(d.dir, fmt.Sprintf("%020d.log", d.sequence))

	w, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	err = syscall.Fallocate(int(w.Fd()), 0, 0, d.maxLogSize)
	if err != nil {
		panic(err)
	}

	r, err := mmap.Open(logFile)
	if err != nil {
		panic(err)
	}

	log := managedLog{
		log:  newLog(r, w),
		ID:   int64(d.sequence),
		File: w,
	}
	d.logs = append(d.logs, log)

	if d.cutHandler != nil && d.sequence != 0 {
		err := d.cutHandler(log.ID)
		if err != nil {
			panic(err)
		}
	}

	d.sequence++

	return log
}

func (d *db) activeLog() managedLog {
	if len(d.logs) == 0 {
		return d.newLog()
	}
	return d.logs[len(d.logs)-1]
}

// TODO: Should close Log's file descriptors here...?
func (d *db) compact() {
	log := d.logs[0]
	id := log.ID

	// The CompactionHandler is run _before_ compaction actually occurs
	// so that the Deck consumer has a full view of the data, including
	// what is being removed. Technically compaction may fail afterwards
	// without the application knowing about it, but we're talking
	// about removing a file and in-memory operations that are well-tested.
	if d.compactHandler != nil {
		err := d.compactHandler(log.Counters())
		if err != nil {
			panic(err)
		}
	}

	logFile := filepath.Join(d.dir, fmt.Sprintf("%020d.log", id))
	err := os.Remove(logFile)
	if err != nil {
		panic(err)
	}

	d.inventory.Remove(log.Counters())

	d.logs = d.logs[1:len(d.logs)]
	d.offset++
}

// pointer returns the location of the Record last written to the Deck.
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

// managedLog represents a Log that is managed by a Deck.
// It wraps the basic Log and adds an ID for tracking unique Logs,
// and the handle of the underlying file.
type managedLog struct {
	*log
	ID   int64
	File *os.File
	mmap.ReaderAt
}
