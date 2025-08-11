package logdeck

import (
	"math/rand/v2"
	"testing"

	. "github.com/robinkb/cascade-registry/testing"
)

func TestCounters(t *testing.T) {
	t.Run("calling add increments counter for that type", func(t *testing.T) {
		c := newCounters()
		want := Type(rand.Uint64())

		c.add(want)
		c.add(want)
		c.add(want)

		for got, count := range c.All() {
			AssertEqual(t, got, want)
			AssertEqual(t, count, 3)
		}
	})

	t.Run("calling add increments the total", func(t *testing.T) {
		c := newCounters()

		want := uint64(10)

		for range want {
			c.add(Type(rand.Uint64()))
		}

		got := c.total()
		AssertEqual(t, got, want)
	})
}

func TestInventory(t *testing.T) {
	inv := newInventory()

	rtype1, pointers1 := randomType(), randomPointers(15)
	for _, ptr := range pointers1 {
		inv.Add(rtype1, ptr)
	}

	rtype2, pointers2 := randomType(), randomPointers(10)
	for _, ptr := range pointers2 {
		inv.Add(rtype2, ptr)
	}

	t.Run("all pointers can be retrieved by record type", func(t *testing.T) {
		for i, want := range pointers1 {
			got, err := inv.Get(rtype1, i)
			AssertNoError(t, err)
			AssertDeepEqual(t, got, want)
		}

		for i, want := range pointers2 {
			got, err := inv.Get(rtype2, i)
			AssertNoError(t, err)
			AssertDeepEqual(t, got, want)
		}
	})

	t.Run("getting pointer with unknown record type returns ErrRecordTypeUnknown", func(t *testing.T) {
		_, err := inv.Get(randomType(), 0)
		AssertErrorIs(t, err, ErrTypeUnknown)
	})

	t.Run("getting pointer with index out of bounds returns ErrIndexOutOfBounds", func(t *testing.T) {
		_, err := inv.Get(rtype1, len(pointers1))
		AssertErrorIs(t, err, ErrIndexOutOfBounds)

		_, err = inv.Get(rtype1, -1)
		AssertErrorIs(t, err, ErrIndexOutOfBounds)
	})

	t.Run("Range returns all pointers for a given type", func(t *testing.T) {
		got, err := inv.Range(rtype1, 0, len(pointers1))
		AssertNoError(t, err)
		AssertDeepEqual(t, got, pointers1)
	})

	t.Run("Range for an unknown record type returns ErrRecordTypeUnknown", func(t *testing.T) {
		_, err := inv.Range(randomType(), 0, 1)
		AssertErrorIs(t, err, ErrTypeUnknown)
	})

	t.Run("Range with invalid ranges returns an error", func(t *testing.T) {
		tc := []struct {
			name   string
			lo, hi int
			want   error
		}{
			{"lo equal to hi", 1, 1, ErrRangeInvalid},
			{"hi lower than lo", 1, 0, ErrRangeInvalid},
			{"negative lo", -1, 1, ErrIndexOutOfBounds},
			{"hi higher than number of pointers", 0, 100, ErrIndexOutOfBounds},
		}

		for _, tt := range tc {
			t.Run(tt.name, func(t *testing.T) {
				_, err := inv.Range(rtype1, tt.lo, tt.hi)
				AssertErrorIs(t, err, tt.want)
			})
		}
	})

	t.Run("Count(t) returns the number of pointers of a given type", func(t *testing.T) {
		got := inv.Count(rtype1)
		AssertEqual(t, got, len(pointers1))

		got = inv.Count(rtype2)
		AssertEqual(t, got, len(pointers2))
	})

	t.Run("Count of an unknown RecordType returns 0", func(t *testing.T) {
		got := inv.Count(randomType())
		AssertEqual(t, got, 0)
	})

	t.Run("remove purges pointers according to given Counters", func(t *testing.T) {
		// Populate Inventory with some pointers for this test.
		rtype := randomType()
		pointers := randomPointers(10)
		for _, ptr := range pointers {
			inv.Add(rtype, ptr)
		}

		// Make sure that all of our pointers are available.
		got, err := inv.Get(rtype, 0)
		AssertNoError(t, err)
		AssertDeepEqual(t, got, pointers[0])

		got, err = inv.Get(rtype, 9)
		AssertNoError(t, err)
		AssertDeepEqual(t, got, pointers[9])

		wantRemoved := 5 // Don't change or comments won't make sense ;-;

		// Simulate a Log that has five records of this type.
		c := newCounters()
		for range wantRemoved {
			c.add(rtype)
		}

		// Now "remove" the Log from the DB.
		inv.Remove(c)

		// Make sure that the last five pointers are still there.
		for i := range len(pointers) - wantRemoved {
			got, err := inv.Get(rtype, i)
			AssertNoError(t, err)
			AssertDeepEqual(t, got, pointers[i+wantRemoved])
		}

		// THere is no sixth pointer.
		_, err = inv.Get(rtype, 5)
		AssertErrorIs(t, err, ErrIndexOutOfBounds)
	})

	t.Run("removing record of unknown type panics", func(t *testing.T) {
		defer AssertPanics(t, ErrTypeUnknown)
		c := newCounters()
		c.add(randomType())

		inv.Remove(c)
	})

	t.Run("removing more records than are available panics", func(t *testing.T) {
		defer AssertPanics(t, ErrInvalidCompaction)
		c := newCounters()
		for range len(pointers1) + 1 {
			c.add(rtype1)
		}

		inv.Remove(c)
	})
}

func randomPointers(n int) []pointer {
	pointers := make([]pointer, n)
	for i := range n {
		pointers[i] = randomPointer()
	}
	return pointers
}

func randomType() Type {
	return Type(rand.Uint64())
}

func randomPointer() pointer {
	return pointer{
		Log:    LogID(rand.Uint64()),
		Offset: rand.Int64(),
		Size:   rand.Int64(),
	}
}
