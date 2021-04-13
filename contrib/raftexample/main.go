// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func main1() {
	// cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady, _ := newRaftNode(*id, nil, *join, getSnapshot, proposeC, confChangeC, nil)
	// strings.Split(*cluster, ",")

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(kvs, *kvport, confChangeC, errorC)
}

func createNode(id int, cluster []int, transport *Transport) *raftNode {
	proposeC := make(chan string)
	// TODO these should live as long as the program does
	// defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	// defer close(confChangeC)
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady, n := newRaftNode(id,
		cluster, false, getSnapshot, proposeC, confChangeC, transport)
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)
	return n
}

func interpret(nodes map[int]*raftNode, events []event) {
	for _, e := range events {
		switch e.Type {
		case Timeout:
			nodes[e.Recipient].node.Campaign(context.TODO())
		default:
			panic(fmt.Sprintf("unknown event type %d", e.Type))
		}
	}
}

func main() {

	// Config
	nodes := 2

	// Wiring
	transport := newTransport()

	cluster := []int{}
	for i := 1; i <= nodes; i++ {
		cluster = append(cluster, i)
	}

	allNodes := map[int]*raftNode{}
	for _, id := range cluster {
		node := createNode(id, cluster, transport)
		transport.AddNode(uint64(id), node)
		allNodes[id] = node
	}
	interpret(allNodes, []event{
		{Type: Timeout, Recipient: 1},
	})
	select {}
}
