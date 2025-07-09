package storage

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/exp/mmap"
)

type DeckConfig struct {
	MaxLogSize  int64
	MaxLogCount int
}

var defaultDeckConfig = DeckConfig{
	MaxLogSize:  64 << 20,
	MaxLogCount: 16,
}

func NewDeck(dir string, c *DeckConfig) Deck {
	d := Deck{
		logs: make([]*Log, 0),

		dir:         dir,
		maxLogSize:  defaultDeckConfig.MaxLogSize,
		maxLogCount: defaultDeckConfig.MaxLogCount,

		records: make(map[RecordType][]Pointer),
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
	// records holds pointers to all known Records in the Deck,
	// organized by type. It grows when appending Records to the Deck,
	// and shrinks when Logs in the Deck are compacted.
	records map[RecordType][]Pointer
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

	if d.records[r.Type] == nil {
		d.records[r.Type] = make([]Pointer, 0)
	}

	d.records[r.Type] = append(d.records[r.Type], d.Pointer())

	return nil
}

func (d *Deck) Get(t RecordType, i uint64, r *Record) error {
	ptr := d.records[t][i]
	return d.readAt(r, ptr)
}

func (d *Deck) Count(t RecordType) int {
	return len(d.records[t])
}

func (d *Deck) First(t RecordType, r *Record) error {
	ptr := d.records[t][0]
	return d.readAt(r, ptr)
}

func (d *Deck) Last(t RecordType, r *Record) error {
	ptr := d.records[t][len(d.records[t])-1]
	return d.readAt(r, ptr)
}

func (d *Deck) Range(t RecordType, lo, hi uint64) iter.Seq2[*Record, error] {
	r := new(Record)
	return func(yield func(*Record, error) bool) {
		for _, ptr := range d.records[t][lo:hi] {
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
			if d.records[record.Type] == nil {
				d.records[record.Type] = make([]Pointer, 0)
			}

			d.records[record.Type] = append(d.records[record.Type], Pointer{
				Log:    log.ID,
				Offset: log.Pointer(),
			})
		}
	}
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

		for t, count := range log.counters {
			d.records[t] = d.records[t][count-1:]
		}

		d.logs = d.logs[1:len(d.logs)]
		d.offset++
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
