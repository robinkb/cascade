package logdeck

import (
	"fmt"
	"os"
)

const (
	TypeRequest Type = iota
	TypeResponse
)

func ExampleDumpLog() {
	// Open a Log created by LogDeckDB.
	f, _ := os.Open("00000000000000000012.log")
	defer f.Close()

	for t, val := range DumpLog(f) {
		switch t {
		case TypeRequest:
			fmt.Println("request:", string(val))
		case TypeResponse:
			fmt.Println("response:", string(val))
		}
	}
}
