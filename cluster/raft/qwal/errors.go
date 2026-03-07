package qwal

import "errors"

var (
	// Codec errors
	ErrShortRead        = errors.New("unable to read full record")
	ErrChecksumMismatch = errors.New("CRC checksums did not match")

	// Inventory errors
	ErrIndexOutOfBounds = errors.New("index out of bounds")
	ErrTypeUnknown      = errors.New("type unknown")
	ErrRangeInvalid     = errors.New("range invalid")

	// QWAL errors
	ErrInvalidCompaction = errors.New("invalid compaction")
	ErrCutHookFailed     = errors.New("cut hook failed")
	ErrCompactHookFailed = errors.New("compact hook failed")
)
