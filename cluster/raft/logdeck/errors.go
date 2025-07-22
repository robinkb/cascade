package logdeck

import "errors"

var (
	ErrShortRead         = errors.New("unable to read full record")
	ErrChecksumMismatch  = errors.New("CRC checksums did not match")
	ErrInvalidCompaction = errors.New("invalid compaction")
	ErrIndexOutOfBounds  = errors.New("index out of bounds")
	ErrRecordTypeUnknown = errors.New("record type unknown")
	ErrRangeInvalid      = errors.New("range invalid")
)
