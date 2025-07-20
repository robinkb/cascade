package storage

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
	enc Encoder
	dec Decoder
	// cursor tracks the position of writes to the log.
	// The log is only ever appended to, and cursor only ever increases.
	cursor int64
	// pointer contains the starting position of the last record
	// written to the log.
	pointer int64
	// lastValueSize is the size of the last Record's Value written to the log.
	lastValueSize int64
	// counters tracks how many records of each type is in the Log.
	counters Counters
}

func (l *Log) Append(r *Record) error {
	n, err := l.enc.Encode(r)
	l.advance(n, r.Type)
	return err
}

func (l *Log) ReadAt(r *Record, offset int64) error {
	_, err := l.dec.RecordAt(r, offset)
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

func (l *Log) Pointer() (int64, int64) {
	return l.pointer + RecordHeaderLength, l.lastValueSize
}

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
