package set

import (
	"bytes"
	"encoding/gob"
	"iter"
)

func New[T comparable]() Of[T] {
	return Of[T]{
		make(map[T]struct{}),
	}
}

type Of[T comparable] struct {
	set map[T]struct{}
}

func (o Of[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for v := range o.set {
			if !yield(v) {
				return
			}
		}
	}
}

func (o Of[T]) Add(v T) {
	o.set[v] = struct{}{}
}

func (o Of[T]) Remove(v T) {
	delete(o.set, v)
}

func (o Of[T]) Contains(v T) bool {
	_, ok := o.set[v]
	return ok
}

func (o Of[T]) IsEmpty() bool {
	return o.Size() == 0
}

func (o Of[T]) Size() int {
	return len(o.set)
}

func (o Of[T]) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(o.set)
	return buf.Bytes(), err
}

func (o Of[T]) UnmarshalBinary(b []byte) error {
	buf := bytes.NewBuffer(b)
	o.set = make(map[T]struct{}, 0)
	return gob.NewDecoder(buf).Decode(&o.set)
}
