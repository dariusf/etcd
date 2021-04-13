package main

import (
	"context"

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

// func (t *Transport) Start() error {
// 	return nil
// }

// func (t *Transport) Handler() http.Handler {
// 	// return http.NewServeMux()
// 	return nil
// }

func (t *Transport) Send(msgs []raftpb.Message) {
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
