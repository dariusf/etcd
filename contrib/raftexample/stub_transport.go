package main

import (
	"context"
	"sync"

	// "net/http"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

type Transport struct {
	ErrorC chan error
	inputs map[uint64]chan raftpb.Message
	raft   map[uint64]*raftNode
}

func newTransport() *Transport {
	t := &Transport{
		ErrorC: make(chan error),
		inputs: make(map[uint64]chan raftpb.Message),
		raft:   make(map[uint64]*raftNode),
	}
	return t
}

func (t *Transport) AddNode(id uint64, r *raftNode) {
	t.inputs[id] = make(chan raftpb.Message)
	t.raft[id] = r
	go t.Handle(id, r)
}

func (t *Transport) Handle(id uint64, r *raftNode) {
	for {
		m := <-t.inputs[id]
		r.Process(context.TODO(), m)
	}
}

// We model the soup because we can't control when the impl sends messages.
// This lets us control when they are received.
var soup []raftpb.Message
var soupL = sync.Mutex{}

// The library calls this
func (t *Transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		soupL.Lock()
		soup = append(soup, m)
		soupL.Unlock()
	}
}

// This blocks until the required messages are in the soup
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
		t.inputs[m.To] <- m
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
