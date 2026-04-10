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
	ErrNotDirectory      = errors.New("path is not a directory")
	ErrInvalidCompaction = errors.New("invalid compaction")
	ErrMustReplay        = errors.New("db must be replayed before use")
	ErrReplayHookFailed  = errors.New("replay hook failed")
	ErrCutHookFailed     = errors.New("cut hook failed")
	ErrCompactHookFailed = errors.New("compact hook failed")
	ErrMissingLogFile    = errors.New("detected missing log file during replay")
)
