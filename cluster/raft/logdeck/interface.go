package logdeck

import "iter"

type (
	// DB represents a sequential collection of values, organized into Logs,
	// and indexed by type and sequence. New values are appended to the active Log.
	// Once the active Log reaches MaxLogSize, a new Log is provisioned to
	// receive new appends. The old Log becomes read-only. Once DB exceeds
	// MaxLogCount, compaction removes the oldest Log and its values.
	DB interface {
		// Append writes a value to the DB.
		Append(t Type, value []byte) error
		// Get retrieves a value with Type t at index i.
		Get(t Type, i int) ([]byte, error)
		// Count returns how many values with Type t are in DB.
		Count(t Type) int
		// First returns the first value of Type t in the DB. The value returned
		// by this method changes after a compaction.
		First(t Type) ([]byte, error)
		// Last returns the last value of Type t that was written to the DB.
		Last(t Type) ([]byte, error)
		// Range returns an iterator that ranges over all values of Type t
		// in the range [lo, hi[.
		Range(t Type, lo, hi int) iter.Seq2[[]byte, error]
		// Cut manually cuts a new Log in the DB. Cutting a Log is normally
		// triggered automatically when MaxLogSize or MaxLogRecordCount is
		// exceeded. Instead, Cut may be used to trigger them manually when more
		// control is required, like for tests. As such, Cut does not consider
		// MaxLogSize and MaxLogRecordCount.
		Cut() error
		// Compact manually triggers a compaction. Compactions are normally
		// triggered automatically when MaxLogCount is exceeded. Instead,
		// Compact may be used to trigger them manually when more control
		// is required, like for tests. As such, Compact does not consider MaxLogCount.
		// Attempting to Compact when the DB only contains one Log returns ErrInvalidCompaction.
		Compact() error
		// CutHook registers CutHookFunc f, which is run whenever a Log is cut.
		// To clear the CutHook, call CutHook with a nil argument.
		CutHook(f CutHookFunc)
		// CompactHook registers CompactHook f, which is run whenever DB compacts a Log.
		// To clear the CompactHook, call CompactHook with a nil argument.
		CompactHook(f CompactHookFunc)
		// Sync calls syscall.Fdatasync on the active Log, ensuring that buffered
		// writes to it are flushed to disk. DB only syncs automatically when a Log
		// is cut and becomes read-only. Any more syncs are the application's responsibility.
		Sync() error
		// Close closes the DB, flushing any pending writes to disk.
		Close() error
	}

	// Type represents the type of a value appended to the DB. Consumers of DB
	// are expected to define their own types as constants of Type.
	Type uint32

	// LogID represents the sequential ID of a Log in the DB.
	LogID uint64

	// CutHookFunc is executed whenever a Log is cut. A Log is cut when it reaches
	// its maximum size and is moved into read-only mode. A new Log is then
	// provisioned to receive new writes. CutHookFunc is executed right after
	// the new Log is provisioned. If Append is called on the DB in
	// CutHookFunc, the appended value is guaranteed to be the first in the new Log.
	CutHookFunc func(id LogID) error

	// CompactHookFunc is executed whenever a Log is compacted. Compaction is
	// triggered when a newly provisioned Log causes MaxLogCount to be exceeded.
	// CompactionHookFunc is executed right before the oldest Log is actually
	// removed from DB, meaning that its data can still be queried for the
	// duration of CompactHookFunc.
	// TODO: Instead of Counters, maybe have it be an iter.Seq2[Type, uint64],
	// the signature of Counters.All(). That way we can make Counters private.
	CompactHookFunc func(c Counters) error

	// Options defines the configurable options of the DB.
	Options struct {
		// MaxLogSize determines the maximum size that a single Log in the DB can have.
		// When appending a value to a Log would make it grow larger than MaxLogSize,
		// a new Log is provisioned, and the value is appended to the new Log.
		// The total maximum DB size on disk is MaxLogSize * (MaxLogCount + 1).
		MaxLogSize int64
		// MaxLogValueCount determines the maximum amount of values that a single Log in the DB can have.
		// When appending a value to a Log would make it exceed MaxLogValueCount,
		// a new Log is provisioned, and the value is appended to the new Log.
		MaxLogValueCount int64
		// MaxLogCount determines how many Logs can be contained in the DB.
		// Once exceeded, the oldest Log in the DB is compacted.
		// The total maximum DB size on disk is MaxLogSize * (MaxLogCount + 1).
		MaxLogCount int
	}
)

// DefaultOptions defines the default Options values.
var DefaultOptions = &Options{
	MaxLogSize:       64 << 20,
	MaxLogValueCount: 10000,
	MaxLogCount:      16,
}
