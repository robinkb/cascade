package storage

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
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

		compactions: make(chan int64),
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

	logFile := filepath.Join(d.dir, fmt.Sprintf("%020d.log", d.sequence))

	w, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	r, err := os.OpenFile(logFile, os.O_RDONLY, 0)
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

func (d *Deck) compact() {
	if len(d.logs) > d.maxLogCount {
		id := d.logs[0].ID
		logFile := filepath.Join(d.dir, fmt.Sprintf("%020d.log", id))
		err := os.Remove(logFile)
		if err != nil {
			panic(err)
		}
		d.logs = d.logs[1:len(d.logs)]
		d.offset++
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
