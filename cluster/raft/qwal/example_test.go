package qwal

import (
	"fmt"
	"os"
)

const (
	// Define our own Types as constants.
	TypeLetter Type = iota
	TypeDigit
)

var (
	letters = []string{
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
		"K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
		"U", "V", "W", "X", "Y", "Z",
	}
	digits = []string{
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
	}
)

func Example() {
	// Open the DB.
	dir, _ := os.MkdirTemp(os.TempDir(), "db")
	defer os.RemoveAll(dir)

	db, err := Open(dir, nil)
	if err != nil {
		panic(err)
	}
	// Always replay the DB before using it.
	db.Replay()
	defer db.Close()

	// Write some values
	for _, letter := range letters {
		db.Append(TypeLetter, []byte(letter))
	}
	for _, digit := range digits {
		db.Append(TypeDigit, []byte(digit))
	}

	firstLetter, _ := db.First(TypeLetter)
	lastLetter, _ := db.Last(TypeLetter)
	fmt.Printf("From %s to %s...\n", firstLetter, lastLetter)

	fmt.Print("As easy as...")
	for d, _ := range db.Range(TypeDigit, 1, 4) {
		fmt.Printf(" %s", d)
	}

	// Output:
	// From A to Z...
	// As easy as... 1 2 3
}
