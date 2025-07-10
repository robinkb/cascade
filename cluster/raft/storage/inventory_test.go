package storage_test

import (
	"math/rand/v2"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	. "github.com/robinkb/cascade-registry/testing"
)

func TestCounters(t *testing.T) {
	c := storage.NewCounters()
	want := storage.RecordType(rand.Uint64())

	c.Add(want)

	for got, count := range c.All() {
		AssertEqual(t, got, want)
		AssertEqual(t, count, 1)
	}
}

func TestInventory(t *testing.T) {
	inv := storage.NewInventory()

	rtype1, pointers1 := randomRecordType(), randomPointers(15)
	for _, ptr := range pointers1 {
		inv.Add(rtype1, ptr)
	}

	rtype2, pointers2 := randomRecordType(), randomPointers(10)
	for _, ptr := range pointers2 {
		inv.Add(rtype2, ptr)
	}

	t.Run("all pointers can be retrieved by record type", func(t *testing.T) {
		for i, want := range pointers1 {
			got, err := inv.Get(rtype1, i)
			AssertNoError(t, err)
			AssertStructsEqual(t, got, want)
		}

		for i, want := range pointers2 {
			got, err := inv.Get(rtype2, i)
			AssertNoError(t, err)
			AssertStructsEqual(t, got, want)
		}
	})

	t.Run("Count(t) returns the expected result", func(t *testing.T) {
		got := inv.Count(rtype1)
		AssertEqual(t, got, len(pointers1))

		got = inv.Count(rtype2)
		AssertEqual(t, got, len(pointers2))
	})

	t.Run("Count of an unknown RecordType returns 0", func(t *testing.T) {
		got := inv.Count(randomRecordType())
		AssertEqual(t, got, 0)
	})

	t.Run("getting pointer with unknown record type returns ErrRecordTypeUnknown", func(t *testing.T) {
		_, err := inv.Get(randomRecordType(), 0)
		AssertErrorIs(t, err, storage.ErrRecordTypeUnknown)
	})

	t.Run("getting pointer with invalid index returns ErrPointerNotFound", func(t *testing.T) {
		_, err := inv.Get(rtype1, len(pointers1))
		AssertErrorIs(t, err, storage.ErrPointerNotFound)

		_, err = inv.Get(rtype1, -1)
		AssertErrorIs(t, err, storage.ErrPointerNotFound)
	})

	t.Run("remove purges pointers according to given Counters", func(t *testing.T) {
		// Populate Inventory with some pointers for this test.
		rtype := randomRecordType()
		pointers := randomPointers(10)
		for _, ptr := range pointers {
			inv.Add(rtype, ptr)
		}

		// Make sure that all of our pointers are available.
		got, err := inv.Get(rtype, 0)
		AssertNoError(t, err)
		AssertStructsEqual(t, got, pointers[0])

		got, err = inv.Get(rtype, 9)
		AssertNoError(t, err)
		AssertStructsEqual(t, got, pointers[9])

		wantRemoved := 5 // Don't change or comments won't make sense ;-;

		// Simulate a Log that has five records of this type.
		c := storage.NewCounters()
		for range wantRemoved {
			c.Add(rtype)
		}

		// Now "remove" the Log from the Deck.
		inv.Remove(c)

		// Make sure that the last five pointers are still there.
		for i := range len(pointers) - wantRemoved {
			got, err := inv.Get(rtype, i)
			AssertNoError(t, err)
			AssertStructsEqual(t, got, pointers[i+wantRemoved])
		}

		// THere is no sixth pointer.
		_, err = inv.Get(rtype, 5)
		AssertErrorIs(t, err, storage.ErrPointerNotFound)
	})

	t.Run("removing record of unknown type panics", func(t *testing.T) {
		defer AssertPanics(t, storage.ErrRecordTypeUnknown)
		c := storage.NewCounters()
		c.Add(randomRecordType())

		inv.Remove(c)
	})

	t.Run("removing more records than are available panics", func(t *testing.T) {
		defer AssertPanics(t, storage.ErrInvalidCompaction)
		c := storage.NewCounters()
		for range len(pointers1) + 1 {
			c.Add(rtype1)
		}

		inv.Remove(c)
	})
}

func randomPointers(n int) []storage.Pointer {
	pointers := make([]storage.Pointer, n)
	for i := range n {
		pointers[i] = randomPointer()
	}
	return pointers
}

func randomRecordType() storage.RecordType {
	return storage.RecordType(rand.Uint64())
}

func randomPointer() storage.Pointer {
	return storage.Pointer{
		Log:    rand.Int64(),
		Offset: rand.Int64(),
	}
}
