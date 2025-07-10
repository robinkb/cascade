package storage

import (
	"fmt"
	"iter"
)

// NewInventory returns an empty Inventory.
func NewInventory() Inventory {
	return Inventory{
		records: make(map[RecordType][]Pointer),
	}
}

// Inventory holds a Pointer to every known record in every Log
// in a Deck, organized by RecordType.
type Inventory struct {
	records map[RecordType][]Pointer
}

// Get returns the Pointer to a Record at the given type and index.
func (inv *Inventory) Get(t RecordType, i int) (Pointer, error) {
	pointers, ok := inv.records[t]
	if !ok {
		return Pointer{}, fmt.Errorf("%w: %d", ErrRecordTypeUnknown, t)
	}

	if len(pointers) <= i || i < 0 {
		return Pointer{}, fmt.Errorf("%w at index: %d", ErrPointerNotFound, i)
	}

	return pointers[i], nil
}

// Count returns the number of Pointers of the given RecordType.
// If the Inventory contains no Pointers of a RecordType,
// it returns 0 instead of panicking.
func (inv *Inventory) Count(t RecordType) int {
	return len(inv.records[t])
}

// Add appends a Pointer of a given RecordType to the Inventory.
func (inv *Inventory) Add(t RecordType, p Pointer) {
	inv.records[t] = append(inv.records[t], p)
}

// Remove purges Records from the Inventory based on the given Counters.
// It is called when a Log is compacted from the Deck, with the Counters
// kept by the Log being compacted. The oldest Logs in the Deck are always
// compacted first, so we can assume that the Records belonging to that Log
// are at the very beginning of the Inventory.
//
// Any error encountered in this process indicates some kind of issue
// in synchronizing the Inventory with the Log contents and should panic.
func (inv *Inventory) Remove(c Counters) {
	for t, count := range c.All() {
		pointers, ok := inv.records[t]
		if !ok {
			panic(fmt.Errorf("%w: %w: %d", ErrInvalidCompaction, ErrRecordTypeUnknown, t))
		}

		if count > uint64(len(pointers)) {
			panic(fmt.Errorf("%w: attempted to compact more records than available", ErrInvalidCompaction))
		}

		inv.records[t] = pointers[count:]
	}
}

// NewCounters returns an empty Counters.
func NewCounters() Counters {
	return Counters{
		counters: make(map[RecordType]uint64),
	}
}

// Counters tracks how many records of each type are in a single Log.
// It is used to update the Inventory in the Deck when a Log is compacted.
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
