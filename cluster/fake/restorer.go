package fake

import (
	"io"

	"github.com/robinkb/cascade/cluster"
)

type Restorer struct {
	Calls uint
	Data  []byte
}

func (rst *Restorer) Restore(r io.Reader, p cluster.Peer) error {
	data, err := io.ReadAll(r)
	rst.Data = data
	return err
}
