package raft

import "io"

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
