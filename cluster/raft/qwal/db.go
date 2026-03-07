package qwal

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
)

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

	for i, name := range logFiles {
		name := filepath.Join(db.dir, name)
		log, err := openLogFile(name)
		if err != nil {
			return nil, err
		}

		// Read the entire log to rebuild our in-memory inventory of records.
		for record := range log.All() {
			offset, size := log.Pointer()

			db.inventory.Add(record.Type, pointer{
				Log:    log.ID,
				Offset: offset,
				Size:   size,
			})
		}

		// Lock all but the last log file.
		if i != len(logFiles)-1 {
			if err := log.Lock(); err != nil {
				return nil, err
			}
		}

		db.logs = append(db.logs, log)
		db.sequence++
	}

	if len(db.logs) == 0 {
		_, err := db.newLog()
		if err != nil {
			return nil, err
		}
	}

	db.offset = uint64(db.logs[0].ID)

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
	// maxLogCount determines the maximum amount of logs stored in the DB.
	// When the number of logs exceeds this number, the oldest log will be compacted.
	maxLogCount int
	// Inventory holds pointers to all known Records in the DB,
	// organized by type. It grows when appending Records to the DB,
	// and shrinks when Logs in the DB are compacted.
	inventory *inventory
	// cutHook is provided by the DB consumer. If provided,
	// it is called by DB after every Log is cut.
	cutHook CutHookFunc
	// compactHook is provided by the DB consumer. If provided,
	// it is called by DB after every compaction, allowing the consumer
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
