package logdeck

import (
	"fmt"
	"sync"
)

// newInventory returns an empty Inventory.
func newInventory() *inventory {
	return &inventory{
		records: make(map[Type][]pointer),
	}
}

// inventory holds a Pointer to every known record in every Log in a DB,
// organized by Type.
type inventory struct {
	mu      sync.RWMutex
	records map[Type][]pointer
}

// Get returns the Pointer to a Record of Type t at index i.
func (inv *inventory) Get(t Type, i int) (pointer, error) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	pointers, ok := inv.records[t]
	if !ok {
		return pointer{}, fmt.Errorf("%w: %d", ErrTypeUnknown, t)
	}

	if len(pointers) <= i || i < 0 {
		return pointer{}, fmt.Errorf("%w: length %d, index %d", ErrIndexOutOfBounds, len(pointers), i)
	}

	return pointers[i], nil
}

func (inv *inventory) Range(t Type, lo, hi int) ([]pointer, error) {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	if lo >= hi {
		return nil, fmt.Errorf("%w: lo %d, hi %d", ErrRangeInvalid, lo, hi)
	}

	pointers, ok := inv.records[t]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrTypeUnknown, t)
	}

	if len(pointers) < hi || lo < 0 {
		return nil, fmt.Errorf("%w: length %d, lo %d, hi %d", ErrIndexOutOfBounds, len(pointers), lo, hi)
	}

	result := make([]pointer, hi-lo)
	for i := range len(result) {
		result[i] = pointers[lo+i]
	}
	return result, nil
}

// Count returns the number of Pointers of the given RecordType.
// If the Inventory contains no Pointers of a RecordType,
// it returns 0 instead of panicking.
func (inv *inventory) Count(t Type) int {
	inv.mu.RLock()
	defer inv.mu.RUnlock()

	return len(inv.records[t])
}

// Add appends a Pointer of a given RecordType to the Inventory.
func (inv *inventory) Add(t Type, p pointer) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	inv.records[t] = append(inv.records[t], p)
}

// Remove purges Records from the Inventory based on the given Counters.
// It is called when a Log is compacted from LogDeck, with the Counters
// kept by the Log being compacted. The oldest Logs in the Deck are always
// compacted first, so we can assume that the Records belonging to that Log
// are at the very beginning of the Inventory.
//
// Any error encountered in this process indicates some kind of issue
// in synchronizing the Inventory with the Log contents and should panic.
func (inv *inventory) Remove(c Counters) {
	inv.mu.Lock()
	defer inv.mu.Unlock()

	for t, count := range c.All() {
		pointers, ok := inv.records[t]
		if !ok {
			panic(fmt.Errorf("%w: %w: %d", ErrInvalidCompaction, ErrTypeUnknown, t))
		}

		if count > uint64(len(pointers)) {
			panic(fmt.Errorf("%w: attempted to compact more records than available", ErrInvalidCompaction))
		}

		inv.records[t] = pointers[count:]
	}
}

// pointer points to the location and size of a record's Value in a log.
type pointer struct {
	// Log is the ID of the log within the DB that the value resides in.
	Log LogID
	// Offset is the position within the log that the value starts at.
	Offset int64
	// Size is the length of the value in bytes.
	Size int64
}
