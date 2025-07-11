package storage

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

type DeckConfig struct {
	// MaxLogSize determines the maximum size that a single Log in the Deck can have.
	// When appending a Record to a Log would make it grow larger than MaxLogSize,
	// a new Log is provisioned, and the Record is appended there.
	// The total maximum Deck size on disk is MaxLogSize * MaxLogCount.
	MaxLogSize int64
	// MaxLogCount determines how many Logs can be contained in the Deck.
	// Once exceeded, the oldest Log in the Deck is compacted.
	// The total maximum Deck size on disk is MaxLogSize * MaxLogCount.
	MaxLogCount int
}

var defaultDeckConfig = DeckConfig{
	MaxLogSize:  64 << 20,
	MaxLogCount: 16,
}

var logNameRe = regexp.MustCompile(`(\d{20}).log`)

func NewDeck(dir string, c *DeckConfig) Deck {
	d := Deck{
		logs: make([]*Log, 0),

		dir:         dir,
		maxLogSize:  defaultDeckConfig.MaxLogSize,
		maxLogCount: defaultDeckConfig.MaxLogCount,

		inventory: NewInventory(),
	}

	if c != nil {
		if c.MaxLogSize != 0 {
			d.maxLogSize = c.MaxLogSize
		}

		if c.MaxLogCount != 0 {
			d.maxLogCount = c.MaxLogCount
		}
	}

	info, err := os.Stat(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(d.dir, 0755)
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

		name := filepath.Join(d.dir, name)
		w, err := os.OpenFile(name, os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		r, err := mmap.Open(name)
		if err != nil {
			panic(err)
		}

		log := NewLog(r, w)
		log.ID = id
		d.logs = append(d.logs, log)
		d.sequence++
	}

	return d
}

type LogIndex struct {
	Log *Log
	ID  uint64
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
	// Inventory holds pointers to all known Records in the Deck,
	// organized by type. It grows when appending Records to the Deck,
	// and shrinks when Logs in the Deck are compacted.
	inventory Inventory
	// compactions signals the consumer of the Deck that a compaction happened.
	// The consumer can user the sent Counters to update its internal bookkeeping.
	// Listening is optional.
	compactions chan Counters
}

func (d *Deck) Append(r *Record) error {
	log := d.activeLog()
	if log.cursor+r.Size() > d.maxLogSize {
		log = d.newLog()
	}

	err := log.Append(r)
	if err != nil {
		return err
	}

	d.inventory.Add(r.Type, d.Pointer())

	return nil
}

func (d *Deck) Get(t RecordType, i int, r *Record) error {
	ptr, err := d.inventory.Get(t, i)
	if err != nil {
		return err
	}

	return d.readAt(r, ptr)
}

func (d *Deck) Count(t RecordType) int {
	return d.inventory.Count(t)
}

func (d *Deck) First(t RecordType, r *Record) error {
	ptr, err := d.inventory.Get(t, 0)
	if err != nil {
		return err
	}

	return d.readAt(r, ptr)
}

func (d *Deck) Last(t RecordType, r *Record) error {
	ptr, err := d.inventory.Get(t, d.inventory.Count(t)-1)
	if err != nil {
		return err
	}

	return d.readAt(r, ptr)
}

func (d *Deck) Range(t RecordType, lo, hi int) iter.Seq2[*Record, error] {
	return func(yield func(*Record, error) bool) {
		pointers, err := d.inventory.Range(t, lo, hi)
		if err != nil {
			yield(nil, err)
			return
		}

		r := new(Record)
		for _, ptr := range pointers {
			err := d.readAt(r, ptr)
			if !yield(r, err) {
				return
			}
		}
	}
}

func (d *Deck) readAt(r *Record, p Pointer) error {
	return d.logs[p.Log-d.offset].ReadAt(r, p.Offset)
}

func (d *Deck) ReadAll() {
	for _, log := range d.logs {
		for record := range log.All() {
			d.inventory.Add(record.Type, Pointer{
				Log:    log.ID,
				Offset: log.Pointer(),
			})
		}
	}
}

func (d *Deck) Compactions() <-chan Counters {
	d.compactions = make(chan Counters)
	return d.compactions
}

func (d *Deck) newLog() *Log {
	defer d.compact()

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

	log := NewLog(r, w)
	log.ID = int64(d.sequence)
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

// TODO: Should close Log's file descriptors here...?
func (d *Deck) compact() {
	if len(d.logs) > d.maxLogCount {
		log := d.logs[0]
		id := log.ID
		logFile := filepath.Join(d.dir, fmt.Sprintf("%020d.log", id))
		err := os.Remove(logFile)
		if err != nil {
			panic(err)
		}

		d.inventory.Remove(log.Counters())

		d.logs = d.logs[1:len(d.logs)]
		d.offset++

		if d.compactions != nil {
			d.compactions <- log.Counters()
		}
	}
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
