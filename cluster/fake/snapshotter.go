package fake

import "io"

type Snapshotter struct {
	Calls uint
	Data  []byte
}

func (s *Snapshotter) Snapshot(w io.Writer) error {
	s.Calls++
	if s.Data == nil {
		s.Data = make([]byte, 0)
	}
	_, err := w.Write(s.Data)
	return err
}
