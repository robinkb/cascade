package raft

import (
	"context"
	"log"
)

type EventReason string

type Event struct {
	// Reason is a machine understandable string
	Reason EventReason
	// Message is a human-readable description of this event.
	Message string
}

const (
	ReasonStateChanged EventReason = "StateChanged"
)

func (n *node) Emit(ctx context.Context, receiver string) <-chan Event {
	if ch, ok := n.receivers[receiver]; ok {
		return ch
	}

	n.receivers[receiver] = make(chan Event, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(n.receivers[receiver])
				delete(n.receivers, receiver)
				return
			}
		}
	}()

	return n.receivers[receiver]
}

func (n *node) emit(e Event) {
	for name, ch := range n.receivers {
		select {
		case ch <- e:
		default:
			log.Printf("dropped event due to slow receiver: %s", name)
		}
	}
}
