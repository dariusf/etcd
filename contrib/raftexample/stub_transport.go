package main

import (
	"context"
	"fmt"
	"sync"

	// "net/http"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

type Transport struct {
	ErrorC chan error
	inputs map[int]chan raftpb.Message
	debug  bool
}

func newTransport(debug bool) *Transport {
	t := &Transport{
		ErrorC: make(chan error),
		inputs: make(map[int]chan raftpb.Message),
		debug:  debug,
	}
	return t
}

func (t *Transport) AddNode(id int, nodes map[int]*raftNode) {
	t.inputs[id] = make(chan raftpb.Message)
	go t.Handle(id, nodes)
}

func (t *Transport) Handle(id int, nodes map[int]*raftNode) {
	for {
		m := <-t.inputs[id]
		nodes[id].Process(context.TODO(), m)
	}
}

// We model the soup because we can't control when the impl sends messages.
// This lets us control when they are received.
var soup []raftpb.Message
var soupL = sync.Mutex{}

// The library calls this
func (t *Transport) Send(msgs []raftpb.Message) {
	soupL.Lock()
	for _, m := range msgs {
		soup = append(soup, m)
		if t.debug {
			fmt.Printf("debug soup: %d -> soup: %s\n", m.From, m.Type)
		}
	}
	soupL.Unlock()
	// t.reallySend(msgs)
}

// This blocks until the required messages are in the soup.
// The predicate given should be as specific as possible, as
// this returns as soon as it becomes true.
func (t *Transport) WaitForMessages(f func(raftpb.Message) bool) []raftpb.Message {
	for {
		// Lock only for the start of each iteration, before sleeping
		soupL.Lock()
		in := []raftpb.Message{}
		out := []raftpb.Message{}
		for _, m := range soup {
			if f(m) {
				in = append(in, m)
			} else {
				out = append(out, m)
			}
		}
		if len(in) > 0 {
			soup = out
			soupL.Unlock()
			return in
		} else {
			soupL.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Like WaitForMessages but only waits for changes, without mutating
func (t *Transport) ObserveSent(f func(raftpb.Message) bool) {
	for {
		in := []raftpb.Message{}
		out := []raftpb.Message{}
		// Lock only when reading soup
		soupL.Lock()
		for _, m := range soup {
			if f(m) {
				in = append(in, m)
			} else {
				out = append(out, m)
			}
		}
		soupL.Unlock()
		if len(in) > 0 {
			return
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (t *Transport) reallySend(msgs []raftpb.Message) {
	for _, m := range msgs {
		t.inputs[int(m.To)] <- m
		if t.debug {
			fmt.Printf("debug soup: soup -> %d: %s\n", m.To, m.Type)
		}
	}
}

func (t *Transport) SendSnapshot(m snap.Message) {

}

func (t *Transport) AddRemote(id types.ID, urls []string) {

}

func (t *Transport) AddPeer(id types.ID, urls []string) {

}

func (t *Transport) RemovePeer(id types.ID) {

}

func (t *Transport) RemoveAllPeers() {

}

func (t *Transport) UpdatePeer(id types.ID, urls []string) {

}

func (t *Transport) ActiveSince(id types.ID) time.Time {
	return time.Time{}
}

func (t *Transport) ActivePeers() int {
	return 0
}

func (t *Transport) Stop() {

}
