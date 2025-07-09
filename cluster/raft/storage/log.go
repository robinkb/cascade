package storage

import (
	"io"
	"iter"
	"log"
)

func NewLog(r io.ReaderAt, w io.Writer) *Log {
	return &Log{
		enc:      NewEncoder(w),
		dec:      NewDecoder(r),
		counters: make(map[RecordType]uint64),
	}
}

type Log struct {
	ID int64

	enc Encoder
	dec Decoder
	// cursor tracks the position of writes to the log.
	// The log is only ever appended to, and cursor only ever increases.
	cursor int64
	// pointer contains the starting position of the last record
	// written to the log.
	pointer int64
	// counters tracks how many records of each type is in the Log.
	// It is used by Deck during compaction to purge its in-memory pointers.
	// Writes to counters are never concurrent and it is only read when
	// the Log is being compacted, so a mutex should not be needed.
	counters map[RecordType]uint64
}

func (l *Log) Append(r *Record) error {
	n, err := l.enc.Encode(r)
	l.advance(n, r.Type)
	return err
}

func (l *Log) ReadAt(r *Record, offset int64) error {
	_, err := l.dec.DecodeAt(r, offset)
	return err
}

func (l *Log) All() iter.Seq[*Record] {
	return func(yield func(*Record) bool) {
		r := new(Record)
		for {
			n, err := l.dec.DecodeAt(r, l.cursor)
			if err != nil {
				if err == io.EOF {
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

func (l *Log) Pointer() int64 {
	return l.pointer
}

func (l *Log) Rewind() {
	l.cursor = 0
	l.pointer = 0
}

func (l *Log) advance(n int64, t RecordType) {
	l.pointer = l.cursor
	l.cursor += n
	l.counters[t]++
}
