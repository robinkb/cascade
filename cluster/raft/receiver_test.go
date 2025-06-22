package raft_test

import (
	"math/rand/v2"
	"net/http/httptest"
	"testing"

	"github.com/robinkb/cascade-registry/cluster/raft"
	. "github.com/robinkb/cascade-registry/testing"
	"github.com/robinkb/cascade-registry/testing/mock"
	"go.etcd.io/raft/v3/raftpb"
)

func TestSendMessage(t *testing.T) {
	want := &raftpb.Message{
		Type: raftpb.MsgBeat,
		From: rand.Uint64(),
		To:   rand.Uint64(),
	}
	node := mock.NewRaftNode(t)
	node.EXPECT().
		Receive(want).
		Return(nil)

	server := raft.NewServer(node)
	ts := httptest.NewServer(server)
	defer ts.Close()

	client := raft.NewClient(ts.URL)

	err := client.SendMessage(want)
	AssertNoError(t, err)

}
