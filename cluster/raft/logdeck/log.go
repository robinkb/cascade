package logdeck

import (
	"errors"
	"fmt"
	"io"
	"iter"
)

func newLog(r io.ReaderAt, w io.Writer) *log {
	return &log{
		enc:      newEncoder(w),
		dec:      newDecoder(r),
		counters: newCounters(),
	}
}

type log struct {
	// enc is the encoder used to append Records to the Log.
	enc *encoder
	// dec is the decoder used to read records or values from the Log.
	dec *decoder
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
}

func (l *log) Append(r *record) error {
	n, err := l.enc.Encode(r)
	l.advance(n, r.Type)
	return err
}

func (l *log) ValueAt(p []byte, offset int64) error {
	_, err := l.dec.ValueAt(p, offset)
	return err
}

func (l *log) All() iter.Seq[*record] {
	return func(yield func(*record) bool) {
		r := new(record)
		for {
			n, err := l.dec.RecordAt(r, l.cursor)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				panic(fmt.Sprintf("error while reading log: %s", err))
			}
			l.advance(n, r.Type)

			if !yield(r) {
				break
			}
		}
	}
}

func (l *log) Counters() Counters {
	return l.counters
}

// I want to get rid of this. It's awkward. Just return it from Append.
func (l *log) Pointer() (int64, int64) {
	return l.pointer + RecordHeaderLength, l.lastValueSize
}

// Also get rid of this. It's only used in a test, and never intended to be used in production.
func (l *log) Rewind() {
	l.cursor = 0
	l.pointer = 0
}

func (l *log) advance(n int64, t Type) {
	l.pointer = l.cursor
	l.cursor += n
	l.lastValueSize = n - RecordHeaderLength
	l.counters.add(t)
}

// newCounters returns an empty Counters.
func newCounters() Counters {
	return Counters{
		counters: make(map[Type]uint64),
	}
}

// Counters tracks how many records of each type are in a single log.
// It is used to update the inventory in the DB when a log is compacted.
type Counters struct {
	counters map[Type]uint64
}

// add increments the counter for the given RecordType by 1.
func (c *Counters) add(t Type) {
	c.counters[t]++
}

// All iterates over all of the counters, returning the RecordType
// and how many Records of this type are in the log.
func (c *Counters) All() iter.Seq2[Type, uint64] {
	return func(yield func(Type, uint64) bool) {
		for t, count := range c.counters {
			if !yield(t, count) {
				return
			}
		}
	}
}
