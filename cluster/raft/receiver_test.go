package raft

import (
	"math/rand/v2"
	"net/http/httptest"
	"testing"

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
	service := mock.NewRaftReceiver(t)
	service.EXPECT().
		Receive(want)

	server := NewServer(service)
	ts := httptest.NewServer(server)
	defer ts.Close()

	client := NewClient(ts.URL)

	err := client.SendMessage(want)
	AssertNoError(t, err)

}
