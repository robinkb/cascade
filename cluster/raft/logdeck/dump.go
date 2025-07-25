package logdeck

import (
	"io"
	"iter"
)

// DumpLog returns an iterator over [io.ReaderAt] r,
// which is expected to be a Log created by DB.
// It can be used to dump the contents of a Log for inspection.
func DumpLog(r io.ReaderAt) iter.Seq2[Type, []byte] {
	l := newLog(r, nil)

	return func(yield func(Type, []byte) bool) {
		for record := range l.All() {
			if !yield(record.Type, record.Value) {
				return
			}
		}
	}
}
