package qwal

import (
	"fmt"
	"os"
)

const (
	TypeRequest Type = iota
	TypeResponse
)

func ExampleDumpLog() {
	// Open a Log created by QWAL.
	f, _ := os.Open("00000000000000000012.log")
	defer f.Close() // nolint: errcheck

	for t, val := range DumpLog(f) {
		switch t {
		case TypeRequest:
			fmt.Println("request:", string(val))
		case TypeResponse:
			fmt.Println("response:", string(val))
		}
	}
}
