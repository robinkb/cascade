package main

import (
	"flag"
	"fmt"
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
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println("error closing file:", err)
		}
	}()

	l := storage.NewLog(f, nil)

	for r := range l.All() {
		switch r.Type {
		case storage.TypeEntry:
			var entry raftpb.Entry
			err = entry.Unmarshal(r.Value)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%020d [entry] index: %d, term %d, type %s\n", l.Pointer(), entry.Index, entry.Term, entry.Type.String())

		case storage.TypeHardState:
			var hs raftpb.HardState
			err = hs.Unmarshal(r.Value)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%020d [state] commit: %d, term %d, vote %d\n", l.Pointer(), hs.Commit, hs.Term, hs.Vote)

		case storage.TypeSnapshot:
			var snap raftpb.Snapshot
			err = snap.Unmarshal(r.Value)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%020d [snapshot]\n", l.Pointer())
		}
	}
}
