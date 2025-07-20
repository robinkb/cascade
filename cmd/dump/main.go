package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/robinkb/cascade-registry/cluster/raft"
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
		case raft.TypeEntry:
			var entry raftpb.Entry
			err = entry.Unmarshal(r.Value)
			if err != nil {
				log.Fatalln(err)
			}
			offset, size := l.Pointer()
			fmt.Printf("%20d:%-6d [entry   ] index %d, term %d, type %s\n", offset, size, entry.Index, entry.Term, entry.Type.String())

		case raft.TypeHardState:
			var hs raftpb.HardState
			err = hs.Unmarshal(r.Value)
			if err != nil {
				log.Fatalln(err)
			}
			offset, size := l.Pointer()
			fmt.Printf("%20d:%-6d [state   ] commit %d, term %d, vote %d\n", offset, size, hs.Commit, hs.Term, hs.Vote)

		case raft.TypeSnapshot:
			var snap raftpb.Snapshot
			err = snap.Unmarshal(r.Value)
			if err != nil {
				log.Fatalln(err)
			}
			offset, size := l.Pointer()
			fmt.Printf("%20d:%-6d [snapshot] index %d, term %d, confState %s\n", offset, size, snap.Metadata.Index, snap.Metadata.Term, snap.Metadata.ConfState.String())
		}
	}
}
