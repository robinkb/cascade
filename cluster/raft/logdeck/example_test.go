package logdeck

import (
	"fmt"
	"os"
)

const (
	// Define our own Types as constants.
	TypeSetup Type = iota
	TypePunchline
)

func Example() {
	// Open the DB.
	dir := os.TempDir()
	db, err := Open(dir, nil)
	if err != nil {
		panic(err)
	}

	// Write some values.
	db.Append(TypeSetup, []byte("Why did the chicken cross the road?")) // nolint: errcheck
	db.Append(TypePunchline, []byte("To get to the other side."))       // nolint: errcheck
	db.Append(TypePunchline, []byte("This is a lame joke."))            // nolint: errcheck

	// Use some of the available methods for reading.
	val, _ := db.Get(TypeSetup, 0)
	fmt.Println(string(val))

	val, _ = db.Last(TypePunchline)
	fmt.Println(string(val))

	val, _ = db.First(TypePunchline)
	fmt.Println(string(val))

	// Output:
	// Why did the chicken cross the road?
	// This is a lame joke.
	// To get to the other side.
}
