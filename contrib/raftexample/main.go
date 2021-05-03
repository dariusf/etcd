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
	"os"
	"reflect"
	"strconv"
	"time"

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

func pause(e event) {
	time.Sleep(1 * time.Second)

	// Alternative to sleeping when interactive

	// scanner := bufio.NewScanner(os.Stdin)
	// scanner.Scan()

	fmt.Printf("----%+v\n", e)
}

func finish() {
	fmt.Printf("----Finished\n")
}

func WaitFor(nodes map[int]*raftNode, f func(nodes map[int]*raftNode) bool) {
	for {
		if f(nodes) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func debugPrint(nodes map[int]*raftNode) {
	for i, n := range nodes {
		fmt.Printf("debug: node %d: %s\n", i, n.node.Raft().Log().Show())
	}
}

func interpret(transport *Transport, nodes map[int]*raftNode, events []event) {
	for _, e := range events {
		pause(e)
		debugPrint(nodes)
		switch e.Type {
		case Timeout:
			nodes[e.Recipient].node.Campaign(context.TODO())
			transport.ObserveSent(func(m raftpb.Message) bool {
				return m.Type == raftpb.MsgVote && m.From == uint64(e.Recipient)
			})
		case Send:
			switch e.Message.Type {
			case RequestVoteReq:
				transport.ObserveSent(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgVote && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				})
			case RequestVoteRes:
				transport.ObserveSent(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgVoteResp && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				})
			default:
				panic(fmt.Sprintf("unknown msg type %s", e.Message.Type))
			}
		case Receive:
			switch e.Message.Type {
			case RequestVoteReq:
				transport.reallySend(transport.WaitForMessages(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgVote && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				}))
			case RequestVoteRes:
				transport.reallySend(transport.WaitForMessages(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgVoteResp && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				}))
			default:
				panic(fmt.Sprintf("unknown msg type %s", e.Message.Type))
			}
		case BecomeLeader:
			WaitFor(nodes, func(nodes map[int]*raftNode) bool {
				for i, n := range nodes {
					if n.node.Raft().IsLeader() && e.Recipient == i {
						return true
					}
				}
				return false
			})
		default:
			panic(fmt.Sprintf("unknown event type %s", e.Type))
		}
	}
	finish()
}

func exampleEvents() []event {
	return []event{
		{Type: Timeout, Recipient: 1},
		{Type: Send, Message: msg{Type: RequestVoteReq}, Sender: 1, Recipient: 2},
		{Type: Receive, Message: msg{Type: RequestVoteReq}, Sender: 1, Recipient: 2},
	}
}

// This should be comparable via deep equality
type absState = bool

func main() {

	// Args
	nodes, _ := strconv.Atoi(os.Args[1])
	traceF := os.Args[2]

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
	trace, events := ParseLog(traceF)

	// Here's the definition of a simple test

	var specState absState = trace[len(trace)-1].State.History.HadNumLeaders > 0

	interpret(transport, allNodes, events)
	// interpret(transport, allNodes, exampleEvents())

	abstract := func(transport *Transport, nodes map[int]*raftNode) absState {
		return true
	}

	implState := abstract(transport, allNodes)

	fmt.Printf("spec state: %#v\nimpl state: %#v\n", specState, implState)
	if !reflect.DeepEqual(specState, implState) {
		os.Exit(1)
	}
	// select {}
}
