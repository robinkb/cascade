package logdeck

import (
	"fmt"
	"iter"
	"sync"
)

// NewInventory returns an empty Inventory.
func NewInventory() *Inventory {
	return &Inventory{
		records: make(map[RecordType][]Pointer),
	}
}

// Inventory holds a Pointer to every known record in every Log
// in a Deck, organized by RecordType.
type Inventory struct {
	mu      sync.RWMutex
	records map[RecordType][]Pointer
}

// Get returns the Pointer to a Record at the given type and index.
func (inv *Inventory) Get(t RecordType, i int) (Pointer, error) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	pointers, ok := inv.records[t]
	if !ok {
		return Pointer{}, fmt.Errorf("%w: %d", ErrRecordTypeUnknown, t)
	}

	if len(pointers) <= i || i < 0 {
		return Pointer{}, fmt.Errorf("%w at index: %d", ErrPointerNotFound, i)
	}

	return pointers[i], nil
}

func (inv *Inventory) Range(t RecordType, lo, hi int) ([]Pointer, error) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	if lo < 0 || lo >= hi {
		return nil, fmt.Errorf("%w: lo [%d], hi [%d]", ErrRangeInvalid, lo, hi)
	}

	pointers, ok := inv.records[t]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrRecordTypeUnknown, t)
	}

	if len(pointers) < hi {
		return nil, fmt.Errorf("%w: hi [%d] out of bounds", ErrRangeInvalid, hi)
	}

	result := make([]Pointer, hi-lo)
	for i := range len(result) {
		result[i] = pointers[lo+i]
	}
	return result, nil
}

// Count returns the number of Pointers of the given RecordType.
// If the Inventory contains no Pointers of a RecordType,
// it returns 0 instead of panicking.
func (inv *Inventory) Count(t RecordType) int {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	return len(inv.records[t])
}

// Add appends a Pointer of a given RecordType to the Inventory.
func (inv *Inventory) Add(t RecordType, p Pointer) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

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
	inv.mu.Lock()
	defer inv.mu.Unlock()

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

// Pointer points to the location and size of a Record's Value in a Log.
type Pointer struct {
	// Log is the ID of the Log within the Deck that Value resides in.
	Log int64
	// Offset is the position within the Log that the Value starts at.
	Offset int64
	// Size is the length of the Value in bytes.
	Size int64
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
