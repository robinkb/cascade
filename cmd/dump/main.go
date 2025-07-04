package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/robinkb/cascade-registry/cluster/raft/storage"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	file string
)

func main() {
	flag.StringVar(&file, "file", "", "file to dump")
	flag.Parse()

	f, err := os.Open(file)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	dec := storage.NewDecoder(f)

	r := new(storage.Record)
	var pos int64
	for {
		n, err := dec.DecodeAt(r, pos)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln(err)
		}

		switch r.Type {
		case storage.TypeEntry:
			var entry raftpb.Entry
			err = entry.Unmarshal(r.Value)
			if err != nil {
				log.Fatalln(err)
			}

			fmt.Printf("%-16d:%8d [entry] index: %d, term %d, type %s\n", pos, n, entry.Index, entry.Term, entry.Type.String())
		case storage.TypeHardState:
			var hs raftpb.HardState
			err = hs.Unmarshal(r.Value)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%16d:%8d [state] commit: %d, term %d, vote %d\n", pos, n, hs.Commit, hs.Term, hs.Vote)
		case storage.TypeSnapshot:
			fmt.Printf("%16d:%8d [snapshot]\n", pos, n)
		}

		pos += n
	}
}
