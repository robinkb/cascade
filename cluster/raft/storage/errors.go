package storage

import "errors"

var (
	ErrChecksumMismatch  = errors.New("CRC checksums did not match")
	ErrInvalidCompaction = errors.New("invalid compaction")
	ErrPointerNotFound   = errors.New("pointer to record not found")
	ErrRecordTypeUnknown = errors.New("record type unknown")
	ErrRangeInvalid      = errors.New("range invalid")
)
