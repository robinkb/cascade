package logdeck

import (
	"errors"
	"io"
	"iter"
	"log"
)

func NewLog(r io.ReaderAt, w io.Writer) *Log {
	return &Log{
		enc:      NewEncoder(w),
		dec:      NewDecoder(r),
		counters: NewCounters(),
	}
}

type Log struct {
	// enc is the Encoder used to append Records to the Log.
	enc Encoder
	// dec is the Decoder used to read Records or values from the Log.
	dec Decoder
	// counters tracks how many records of each type are in the Log.
	counters Counters
	// cursor tracks the position of writes to the Log.
	// The Log is only ever appended to, and cursor only ever increases.
	cursor int64
	// pointer contains the starting position of the last record
	// written to the Log.
	pointer int64
	// lastValueSize is the size of the last Record's Value written to the Log.
	lastValueSize int64

	// TODO: Implement this. Provide a Lock() function that sets this to true.
	// Counters cannot be retrieved until the Log is locked.
	// Append cannot be called on a locked Log.
	locked bool
}

func (l *Log) Append(r *Record) error {
	n, err := l.enc.Encode(r)
	l.advance(n, r.Type)
	return err
}

func (l *Log) ValueAt(p []byte, offset int64) error {
	_, err := l.dec.ValueAt(p, offset)
	return err
}

func (l *Log) All() iter.Seq[*Record] {
	return func(yield func(*Record) bool) {
		r := new(Record)
		for {
			n, err := l.dec.RecordAt(r, l.cursor)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				log.Panicln("error while reading log:", err)
			}
			l.advance(n, r.Type)

			if !yield(r) {
				break
			}
		}
	}
}

func (l *Log) Counters() Counters {
	return l.counters
}

// I want to get rid of this. It's awkward. Just return it from Append.
func (l *Log) Pointer() (int64, int64) {
	return l.pointer + RecordHeaderLength, l.lastValueSize
}

// Also get rid of this. It's only used in a test, and never intended to be used in production.
func (l *Log) Rewind() {
	l.cursor = 0
	l.pointer = 0
}

func (l *Log) advance(n int64, t RecordType) {
	l.pointer = l.cursor
	l.cursor += n
	l.lastValueSize = n - RecordHeaderLength
	l.counters.Add(t)
}

// NewCounters returns an empty Counters.
func NewCounters() Counters {
	return Counters{
		counters: make(map[RecordType]uint64),
	}
}

// Counters tracks how many records of each type are in a single Log.
// It is used to update the Inventory in the LogDeck when a Log is compacted.
type Counters struct {
	counters map[RecordType]uint64
}

// Add increments the counter for the given RecordType by 1.
func (c *Counters) Add(t RecordType) {
	c.counters[t]++
}

// All iterates over all of the counters, returning the RecordType
// and how many Records of this type are in the Log.
func (c *Counters) All() iter.Seq2[RecordType, uint64] {
	return func(yield func(RecordType, uint64) bool) {
		for t, count := range c.counters {
			if !yield(t, count) {
				return
			}
		}
	}
}
