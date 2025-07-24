package logdeck

import (
	"io"
	"iter"
)

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
