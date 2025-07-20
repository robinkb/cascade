package raft

import "io"

// SnapshotRestorer combines the basic Snapshotter and Restorer interfaces.
type SnapshotRestorer interface {
	Snapshotter
	Restorer
}

// Snapshotter wraps the basic Snapshot method.
type Snapshotter interface {
	// Snapshot writes a snapshot of the application's state machine to the given Writer.
	Snapshot(w io.Writer) error
}

// Restorer wraps the basic Restore method.
type Restorer interface {
	// Restorer reads a snapshot from the given Reader and applies it to the application's state machine.
	Restore(r io.Reader) error
}

type SpySnapshotter struct {
	CallStats struct {
		Snapshot int
		Restore  int
	}
}

func (s *SpySnapshotter) Snapshot(w io.Writer) error {
	s.CallStats.Snapshot++
	_, err := w.Write([]byte("dummy snapshot"))
	return err
}

func (s *SpySnapshotter) Restore(r io.Reader) error {
	s.CallStats.Restore++
	_, err := io.Copy(io.Discard, r)
	return err
}
