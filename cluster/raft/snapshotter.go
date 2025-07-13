package raft

import "io"

type Snapshotter interface {
	Snapshot(w io.Writer) error
	Restore(r io.Reader) error
}

type DummySnapshotter struct{}

func (s *DummySnapshotter) Snapshot(w io.Writer) error {
	_, err := w.Write([]byte("dummy snapshot"))
	return err
}

func (s *DummySnapshotter) Restore(r io.Reader) error {
	_, err := io.Copy(io.Discard, r)
	return err
}
