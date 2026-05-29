package raft

import (
	"iter"
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

func (n *node) Subscribe(name string) iter.Seq[Event] {
	if _, ok := n.subscribers[name]; ok {
		log.Panicf("subscriber %q already exists", name)
	}

	n.subscribers[name] = make(chan Event, 1)

	return func(yield func(Event) bool) {
		for e := range n.subscribers[name] {
			if !yield(e) {
				close(n.subscribers[name])
				delete(n.subscribers, name)
				return
			}
		}
	}
}
func (n *node) emit(e Event) {
	for name, ch := range n.subscribers {
		select {
		case ch <- e:
		default:
			log.Printf("dropped event due to slow subscriber: %s", name)
		}
	}
}
