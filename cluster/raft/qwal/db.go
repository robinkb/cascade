package qwal

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
)

// Open intializes a [DB] in directory dir. If the directory already contains
// Logs belonging to a [DB], the [DB] reads the logs to restore its in-memory state.
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
		if !info.IsDir() {
			return nil, fmt.Errorf("%w: %s", ErrNotDirectory, info.Name())
		}
	}

	return &db, nil
}

// db implements the [DB] interface.
type db struct {
	logs []*logFile
	// dir is the directory where the log files are stored on-disk.
	dir string
	// sequence holds the ID of the last log created.
	sequence uint64
	// offset translates on-disk log indices to their in-memory equivalent.
	offset uint64
	// maxLogSize determines the maximum size that a log can have.
	// If a Record will cause a log to exceed its maximum allowed size,
	// a new log will be provisioned and the record will be appended there.
	maxLogSize int64
	// maxLogRecordCount determines the maximum amount of records that a log can have.
	// If a record will cause a log to exceed its maximum allowed size,
	// a new log will be provisioned and the Record will be appended there.
	maxLogRecordCount int64
	// maxLogCount determines the maximum amount of logs stored in the DB.
	// When the number of logs exceeds this number, the oldest log will be compacted.
	maxLogCount int
	// Inventory holds pointers to all known records in the DB,
	// organized by type. It grows when appending records to the DB,
	// and shrinks when logs in the DB are compacted out.
	inventory *inventory
	// replayed tracks if the DB has been replayed.
	replayed bool
	// replayHook is provided by the DB consumer. If provided,
	// it is called after reading every record during DB replay.
	replayHookFunc ReplayHookFunc
	// cutHook is provided by the DB consumer. If provided,
	// it is called by DB after every Log is cut.
	cutHook CutHookFunc
	// compactHook is provided by the DB consumer. If provided,
	// it is called by DB after every compaction, allowing the consumer
	// to update its internal bookkeeping.
	compactHook CompactHookFunc
}

// Append implements [DB.Append].
func (d *db) Append(t Type, value []byte) error {
	d.panicIfNotReplayed()

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

	offset, err := log.Append(r)
	if err != nil {
		return err
	}

	d.inventory.Add(r.Type, pointer{
		Log:    log.ID,
		Offset: offset,
		Size:   int64(len(value)),
	})

	// Run compaction _after_ adding the new entry so that
	// the compaction handler has an up-to-date view of the DB.
	if len(d.logs) > d.maxLogCount {
		return d.compact()
	}

	return nil
}

func (d *db) appendWouldExceedLimits(log *logFile, r *record) bool {
	return d.maxLogSize < log.cursor+r.size() ||
		uint64(d.maxLogRecordCount) <= log.counters.total()
}

func (d *db) Get(t Type, i uint64) ([]byte, error) {
	d.panicIfNotReplayed()

	ptr, err := d.inventory.Get(t, i)
	if err != nil {
		return nil, fmt.Errorf("could not get pointer to record: %w", err)
	}

	return d.valueAt(ptr)
}

// Count implements [DB.Count].
func (d *db) Count(t Type) uint64 {
	d.panicIfNotReplayed()
	return d.inventory.Count(t)
}

// First implements [DB.First].
func (d *db) First(t Type) ([]byte, error) {
	d.panicIfNotReplayed()
	ptr, err := d.inventory.Get(t, 0)
	if err != nil {
		return nil, err
	}

	return d.valueAt(ptr)
}

// Last implements [DB.Last].
func (d *db) Last(t Type) ([]byte, error) {
	d.panicIfNotReplayed()
	ptr, err := d.inventory.Get(t, d.inventory.Count(t)-1)
	if err != nil {
		return nil, err
	}

	return d.valueAt(ptr)
}

// Range implements [DB.Range].
func (d *db) Range(t Type, lo, hi uint64) iter.Seq2[[]byte, error] {
	d.panicIfNotReplayed()
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

// Replay implements [DB.Replay].
func (d *db) Replay() error {
	if d.replayed {
		return nil
	}

	logFiles, err := discoverLogFiles(d.dir)
	if err != nil {
		return err
	}

	for i, name := range logFiles {
		name := filepath.Join(d.dir, name)
		log, err := openLogFile(name)
		if err != nil {
			return err
		}

		if d.sequence != 0 && d.sequence+1 != uint64(log.ID) {
			return fmt.Errorf("%w: %d", ErrMissingLogFile, d.sequence+1)
		}

		// Read the entire log to rebuild our in-memory inventory of records.
		for offset, record := range log.All() {
			d.inventory.Add(record.Type, pointer{
				Log:    log.ID,
				Offset: offset,
				Size:   int64(len(record.Value)),
			})

			if d.replayHookFunc != nil {
				if err := d.replayHookFunc(record.Type, record.Value); err != nil {
					return fmt.Errorf("%w: %w", ErrReplayHookFailed, err)
				}
			}
		}

		// Lock all but the last log file.
		if i != len(logFiles)-1 {
			if err := log.Lock(); err != nil {
				return err
			}
		}

		d.logs = append(d.logs, log)
		d.sequence = uint64(log.ID)
	}

	if len(d.logs) == 0 {
		_, err := d.newLog()
		if err != nil {
			return err
		}
	}

	d.offset = uint64(d.logs[0].ID)
	d.replayed = true

	return nil
}

// ReplayHook implements [DB.ReplayHook].
func (d *db) ReplayHook(f ReplayHookFunc) {
	d.replayHookFunc = f
}

func (d *db) panicIfNotReplayed() {
	if !d.replayed {
		panic(ErrMustReplay)
	}
}

// Cut implements [DB.Cut].
func (d *db) Cut() error {
	d.panicIfNotReplayed()
	_, err := d.cut()
	return err
}

// CutHook implements [DB.CutHook].
func (d *db) CutHook(f CutHookFunc) {
	d.cutHook = f
}

// Compact implements [DB.Compact].
func (d *db) Compact() error {
	d.panicIfNotReplayed()
	return d.compact()
}

// CompactHook implements [DB.CompactHook].
func (d *db) CompactHook(f CompactHookFunc) {
	d.compactHook = f
}

func (d *db) Discard() error {
	d.panicIfNotReplayed()

	for range len(d.logs) - 1 {
		err := d.remove(d.logs[0])
		if err != nil {
			return err
		}
	}

	return nil
}

// Status implements [DB.Status].
func (d *db) Status() Status {
	return Status{
		LogCount: len(d.logs),
		Options: Options{
			MaxLogSize:       d.maxLogSize,
			MaxLogValueCount: d.maxLogSize,
			MaxLogCount:      d.maxLogCount,
		},
	}
}

// Sync implements [DB.Sync].
func (d *db) Sync() error {
	d.panicIfNotReplayed()
	return d.activeLog().Sync()
}

// Close implements [DB.Close].
func (d *db) Close() error {
	d.panicIfNotReplayed()
	for _, log := range d.logs[:len(d.logs)-1] {
		if err := log.Close(); err != nil {
			return err
		}
	}
	if err := d.activeLog().Sync(); err != nil {
		return err
	}

	return d.activeLog().Close()
}

func (d *db) activeLog() *logFile {
	return d.logs[len(d.logs)-1]
}

func (d *db) newLog() (*logFile, error) {
	log, err := createLogFile(d.dir, LogID(d.sequence), d.maxLogSize)
	if err != nil {
		return nil, err
	}

	d.logs = append(d.logs, log)
	d.sequence++

	return log, nil
}

func (d *db) cut() (*logFile, error) {
	oldLog := d.activeLog()
	err := oldLog.Lock()
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
	// so that the DB consumer has a full view of the data, including
	// what is being removed. Any errors encountered by compaction are surfaced
	// to the application.
	if d.compactHook != nil {
		counters := log.Counters()
		err := d.compactHook(counters.All())
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCompactHookFailed, err)
		}
	}

	return d.remove(log)
}

func (d *db) remove(log *logFile) error {
	if err := log.Close(); err != nil {
		return err
	}

	if err := os.Remove(log.Writer.Name()); err != nil {
		return err
	}

	d.inventory.Remove(log.Counters())

	d.logs = d.logs[1:len(d.logs)]
	d.offset++

	return nil
}
