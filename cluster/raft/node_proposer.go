package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/robinkb/cascade/cluster"
)

func (n *node) Handle(t cluster.ProposalType, f cluster.ProposalFunc) {
	if f := n.proposalHandlers[t]; f != nil {
		panic(fmt.Errorf("%w: %d", cluster.ErrDuplicateProposalType, t))
	}
	n.proposalHandlers[t] = f
}

func (n *node) Propose(t cluster.ProposalType, data []byte) (resp any, err error) {
	id, await := n.proposalReporter.Await()
	defer n.proposalReporter.Close(id)

	enc := encodeProposal(id, uint32(t), data)
	if err := n.raft.Propose(context.TODO(), enc); err != nil {
		return nil, err
	}

	result := <-await
	return result.resp, result.err
}

func encodeProposal(id uint64, t uint32, data []byte) []byte {
	header := make([]byte, 12)
	binary.LittleEndian.PutUint64(header, id)
	binary.LittleEndian.PutUint32(header[8:], uint32(t))

	buf := new(bytes.Buffer)
	buf.Write(header)
	buf.Write(data)
	return buf.Bytes()
}

func (n *node) applyProposal(data []byte) {
	id, pt, dec := decodeProposal(data)

	f, ok := n.proposalHandlers[cluster.ProposalType(pt)]
	if !ok {
		panic(fmt.Errorf("%w: %d", cluster.ErrUnknownProposalType, pt))
	}

	resp, err := f(dec)

	send, ok := n.proposalReporter.Send(id)
	if ok {
		send <- result{resp, err}
	} else if err != nil {
		log.Println("application error applying proposal:", err)
	}
}

func decodeProposal(enc []byte) (id uint64, t uint32, data []byte) {
	id = binary.LittleEndian.Uint64(enc[0:8])
	t = binary.LittleEndian.Uint32(enc[8:12])
	data = enc[12:]
	return
}

type result struct {
	resp any
	err  error
}
