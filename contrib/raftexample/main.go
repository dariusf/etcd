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
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
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

type interpreter struct {
	transport *Transport
	nodes     map[int]*raftNode

	// Assume that only one conf change can take place at a time in traces
	confChange *raftpb.ConfChange

	// Each node should have two corresponding channels, for closing them
	proposeC map[int]chan string
	errorC   map[int]<-chan error
}

func (itp *interpreter) createNode(id int, cluster []int, restarted bool) {
	proposeC := make(chan string)
	// TODO these should live as long as the program does
	// defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	// defer close(confChangeC)
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady, n := newRaftNode(id,
		cluster, restarted, getSnapshot, proposeC, confChangeC, itp.transport)
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	itp.nodes[id] = n
	itp.proposeC[id] = proposeC
	itp.errorC[id] = errorC
}

func pause(e event, debug bool) {
	if debug {
		// time.Sleep(1 * time.Second)

		// Alternative to sleeping when interactive

		// scanner := bufio.NewScanner(os.Stdin)
		// scanner.Scan()
	}

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

	keys := make([]int, 0, len(nodes))
	for k := range nodes {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, i := range keys {
		fmt.Printf("debug state: node %d: %s\n", i, nodes[i].node.Raft().Log().Show())
	}
}

// Given a set of messages (represented as a function) sent from a leader
// to everyone else (the followers), allows them to pass through the soup.
func (i interpreter) clearBroadcast(leader int, f func(m raftpb.Message, follower int) bool) {
	msgs := []raftpb.Message{}
	for follower := range i.nodes {
		if follower == leader {
			continue
		}
		for _, m := range i.transport.WaitForMessages(func(m raftpb.Message) bool {
			return f(m, follower)
		}) {
			msgs = append(msgs, m)
		}
	}
	i.transport.reallySend(msgs)
}

// AppendEntries messages with empty entries (presumably the base case
// of the leader catching everyone else up) or an entry with empty data
// (from the leader adding an empty entry to its own log on winning an
// election) may appear. This allows them all through.
func (itp *interpreter) clearEmptyAppendEntries(leader int) {

	itp.clearBroadcast(leader, func(m raftpb.Message, follower int) bool {
		return m.Type == raftpb.MsgApp && m.From == uint64(leader) && m.To == uint64(follower) &&
			(len(m.Entries) == 0 || len(m.Entries) == 1 && len(m.Entries[0].Data) == 0)
	})

	// We also have to wait for the responses or they block progress
	// (subsequent MsgApps)

	itp.clearBroadcast(leader, func(m raftpb.Message, follower int) bool {
		return m.Type == raftpb.MsgAppResp && m.From == uint64(follower) && m.To == uint64(leader) &&
			(len(m.Entries) == 0 || len(m.Entries) == 1 && len(m.Entries[0].Data) == 0)
	})
}

func (itp *interpreter) restartNode(id int) {

	// Remember current configuration to pass it back in later
	cluster := itp.nodes[id].peers

	// Shut the node down, as newRaftNode tells us to do.
	// This in turn closes i.nodes[id].stopc, which causes
	// raftNode.stop to be called. All of this blocks
	// until cleanup is done, as we want.

	close(itp.proposeC[id])
	<-itp.errorC[id]

	// Recreating this calls RestartNode down the line, which
	// acts differently depending on if the WAL is present
	itp.createNode(id, cluster, true)
}

func (itp *interpreter) interpret(events []event, debug bool) {
	for _, e := range events {
		pause(e, debug)
		if debug {
			debugPrint(itp.nodes)
		}
		switch e.Type {
		case Timeout:
			itp.nodes[e.Recipient].node.Campaign(context.TODO())
			itp.transport.ObserveSent(func(m raftpb.Message) bool {
				return m.Type == raftpb.MsgVote && m.From == uint64(e.Recipient)
			})
		case Send:
			switch e.Message.Type {
			case RequestVoteReq:
				itp.transport.ObserveSent(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgVote && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				})
			case RequestVoteRes:
				itp.transport.ObserveSent(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgVoteResp && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				})
			case AppendEntriesReq:

				bs := serializeValue(e.Message.Entry.normalValue)
				itp.nodes[e.Sender].node.Propose(context.TODO(), bs)

				itp.transport.ObserveSent(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgApp && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient) &&
						(len(m.Entries) == 0 || bytes.Equal(m.Entries[0].Data, bs))
				})
			case AppendEntriesRes:
				itp.transport.ObserveSent(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgAppResp && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				})
			default:
				log.Fatalf("unknown msg type %s", e.Message.Type)
			}
		case Receive:
			switch e.Message.Type {
			case RequestVoteReq:
				itp.transport.reallySend(itp.transport.WaitForMessages(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgVote && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				}))
			case RequestVoteRes:
				itp.transport.reallySend(itp.transport.WaitForMessages(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgVoteResp && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				}))
			case AppendEntriesReq:
				itp.transport.reallySend(itp.transport.WaitForMessages(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgApp && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				}))
			case AppendEntriesRes:
				itp.transport.reallySend(itp.transport.WaitForMessages(func(m raftpb.Message) bool {
					return m.Type == raftpb.MsgAppResp && m.From == uint64(e.Sender) && m.To == uint64(e.Recipient)
				}))
			default:
				log.Fatalf("unknown msg type %s", e.Message.Type)
			}
		case BecomeLeader:
			leader := e.Recipient
			WaitFor(itp.nodes, func(nodes map[int]*raftNode) bool {
				for i, n := range nodes {
					if n.node.Raft().IsLeader() && leader == i {
						return true
					}
				}
				return false
			})

			// For the empty entries leaders add to their own logs
			itp.clearEmptyAppendEntries(leader)

			// Presumably the base case of leaders catching followers up
			itp.clearEmptyAppendEntries(leader)
		case Restart:

			itp.restartNode(e.Recipient)

			// There's no obvious way to synchronize here, even
			// though it might take some time for the node to come
			// up. That should be harmless, though, as other actions
			// are synchronized.

		case TryRemoveServer:
			if itp.confChange != nil {
				log.Fatalf("no more than one conf change may be attempted at once")
			}
			cc := &raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: uint64(e.Sender),
			}
			itp.confChange = cc
			itp.nodes[e.Recipient].node.ProposeConfChange(context.TODO(), cc)
			// TODO synchronization?

		case RemoveServer:
			if itp.confChange == nil {
				log.Fatalf("no prior attempt to try removing server before actually removing")
			}
			itp.nodes[e.Recipient].node.ApplyConfChange(itp.confChange)
			itp.confChange = nil
			// TODO synchronization?

		default:
			log.Fatalf("unknown event type %s", e.Type)
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

func main() {

	nodes_ := flag.Int("nodes", 0, "number of nodes in the cluster")
	debug_ := flag.Bool("debug", false, "debug mode")
	traceF_ := flag.String("file", "", "join an existing cluster")
	flag.Parse()

	nodes := *nodes_
	debug := *debug_
	traceF := *traceF_

	if nodes == 0 || traceF == "" {
		log.Fatalf("nodes and file are mandatory")
	}

	// Wiring
	itp := interpreter{
		transport: newTransport(debug),
		nodes:     map[int]*raftNode{},
		proposeC:  map[int]chan string{},
		errorC:    map[int]<-chan error{},
	}

	cluster := []int{}
	for i := 1; i <= nodes; i++ {
		cluster = append(cluster, i)
	}

	for _, id := range cluster {
		itp.createNode(id, cluster, false)
		itp.transport.AddNode(id, itp.nodes)
	}
	trace, events := ParseTrace(traceF)

	// Here's the definition of a simple test

	var specState absState = absState{
		atLeastOneLeader: trace[len(trace)-1].State.History.HadNumLeaders > 0,
		logs:             convertAbsLog(trace[len(trace)-1].State.Log),
	}

	itp.interpret(events, debug)
	// interpret(exampleEvents(), debug)

	implState := abstract(itp.transport, itp.nodes)

	fmt.Printf("spec state: %s\n\nimpl state: %s\n", specState, implState)
	if !reflect.DeepEqual(specState, implState) {
		os.Exit(1)
	}
	// select {}
}
