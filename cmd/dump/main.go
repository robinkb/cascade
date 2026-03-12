package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/robinkb/cascade/cluster/raft"
	"github.com/robinkb/cascade/cluster/raft/qwal"
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

	cursor := 0

	for t, val := range qwal.DumpLog(f) {
		switch t {
		case raft.TypeEntry:
			var entry raftpb.Entry
			err = entry.Unmarshal(val)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%8d:%-7d [entry   ] index %d, term %d, type %s\n", cursor, len(val), entry.Index, entry.Term, entry.Type.String())

		case raft.TypeHardState:
			var hs raftpb.HardState
			err = hs.Unmarshal(val)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%8d:%-7d [state   ] commit %d, term %d, vote %d\n", cursor, len(val), hs.Commit, hs.Term, hs.Vote)

		case raft.TypeSnapshot:
			var snap raftpb.Snapshot
			err = snap.Unmarshal(val)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%8d:%-7d [snapshot] index %d, term %d, confState %s\n", cursor, len(val), snap.Metadata.Index, snap.Metadata.Term, snap.Metadata.ConfState.String())
		}

		cursor += qwal.RecordHeaderLength + len(val)
	}
}
