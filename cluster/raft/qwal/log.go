package qwal

import (
	"errors"
	"fmt"
	"io"
	"iter"
)

func newLog(r io.ReaderAt, w io.WriteSeeker) *log {
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
	counters counters
	// cursor tracks the position of writes to the Log.
	// The Log is only ever appended to, and cursor only ever increases.
	cursor int64
}

// Append writes the record using the log's encoder.
// It returns the position of the record's value in the log.
func (l *log) Append(r *record) (offset int64, err error) {
	offset = l.cursor + RecordHeaderLength
	n, err := l.enc.Encode(r)
	l.advance(n, r.Type)
	return
}

func (l *log) ValueAt(p []byte, offset int64) error {
	_, err := l.dec.ValueAt(p, offset)
	return err
}

func (l *log) All() iter.Seq2[int64, *record] {
	return func(yield func(offset int64, r *record) bool) {
		defer func() {
			// Move the encoder's internal cursor to the end of the log
			// after reading all records within it. Ensures that new appends
			// are appended at the end of the log, and existing records
			// are not overwritten.
			if _, err := l.enc.Seek(l.cursor, io.SeekStart); err != nil {
				panic("failed to seek encoder")
			}
		}()

		r := new(record)
		for {
			n, err := l.dec.RecordAt(r, l.cursor)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				panic(fmt.Sprintf("error while reading log: %s", err))
			}
			// Fetch the offset from the current cursor position.
			offset := l.cursor + RecordHeaderLength
			// Cursor is advanced to the next position.
			l.advance(n, r.Type)

			if !yield(offset, r) {
				break
			}
		}
	}
}

func (l *log) Counters() counters {
	return l.counters
}

func (l *log) advance(n int64, t Type) {
	l.cursor += n
	l.counters.add(t)
}

// newCounters returns an empty counters.
func newCounters() counters {
	return counters{
		counters: make(map[Type]uint64),
	}
}

// counters tracks how many records of each type are in a single log.
// It is used to update the inventory in the DB when a log is compacted.
type counters struct {
	counters map[Type]uint64
	records  uint64
}

// add increments the counter for the given Type by 1.
func (c *counters) add(t Type) {
	c.counters[t]++
	c.records++
}

// total returns the total amount of records counted.
func (c *counters) total() uint64 {
	return c.records
}

// All iterates over all of the counters, returning the Type
// and how many Records of this type are in the log.
func (c *counters) All() Counters {
	return func(yield func(Type, uint64) bool) {
		for t, count := range c.counters {
			if !yield(t, count) {
				return
			}
		}
	}
}
