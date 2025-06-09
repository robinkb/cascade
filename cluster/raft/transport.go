package raft

import "go.etcd.io/raft/v3/raftpb"

type (
	// Receiver receives messages.
	Receiver interface {
		Receive() raftpb.Message
	}

	// Sender maintains a connection for sending messages.
	Sender interface {
		Send(m raftpb.Message)
	}
)
