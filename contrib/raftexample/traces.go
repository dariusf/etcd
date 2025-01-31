package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"go.etcd.io/etcd/raft/v3/raftpb"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type EventType int

const (
	Timeout EventType = iota
	Send
	Receive
	BecomeLeader
	Restart
	TryRemoveServer
	RemoveServer
)

func (s EventType) String() string {
	return toString[s]
}

var toString = map[EventType]string{
	Timeout:         "Timeout",
	Send:            "Send",
	Receive:         "Receive",
	BecomeLeader:    "BecomeLeader",
	Restart:         "Restart",
	TryRemoveServer: "TryRemoveServer",
	RemoveServer:    "RemoveServer",
}

var toID = map[string]EventType{
	"Timeout":         Timeout,
	"Send":            Send,
	"Receive":         Receive,
	"BecomeLeader":    BecomeLeader,
	"Restart":         Restart,
	"TryRemoveServer": TryRemoveServer,
	"RemoveServer":    RemoveServer,
}

func (s EventType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(toString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

func (s *EventType) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	*s = toID[j]
	return nil
}

type MessageType int

const (
	RequestVoteReq MessageType = iota
	RequestVoteRes
	AppendEntriesReq
	AppendEntriesRes
)

func (s MessageType) String() string {
	return rvToString[s]
}

var rvToString = map[MessageType]string{
	RequestVoteReq:   "RequestVoteReq",
	RequestVoteRes:   "RequestVoteRes",
	AppendEntriesReq: "AppendEntriesReq",
	AppendEntriesRes: "AppendEntriesRes",
}

var rvToID = map[string]MessageType{
	"RequestVoteReq":   RequestVoteReq,
	"RequestVoteRes":   RequestVoteRes,
	"AppendEntriesReq": AppendEntriesReq,
	"AppendEntriesRes": AppendEntriesRes,
}

func (s MessageType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(rvToString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

func (s *MessageType) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	*s = rvToID[j]
	return nil
}

type EntryType int

const (
	ConfigEntry EntryType = iota
	ValueEntry
)

func (s EntryType) String() string {
	return etToString[s]
}

var etToString = map[EntryType]string{
	ConfigEntry: "ConfigEntry",
	ValueEntry:  "ValueEntry",
}

var etToID = map[string]EntryType{
	"ConfigEntry": ConfigEntry,
	"ValueEntry":  ValueEntry,
}

func (s EntryType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(etToString[s])
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

func (s *EntryType) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	*s = etToID[j]
	return nil
}

type entryValue = int

type lentry struct {
	Term int       `json:"term"`
	Type EntryType `json:"type"`

	// A crude sum. We deserialise Value into one
	// of these fields based on Type
	Value       json.RawMessage `json:"value"`
	normalValue entryValue
	confValue   []int
}

func serializeValue(v entryValue) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(v))
	return bs
}

func deserializeValue(v []byte) entryValue {
	return int(binary.LittleEndian.Uint32(v))
}

func (l lentry) String() string {
	var v string

	switch l.Type {
	case ConfigEntry:
		v = fmt.Sprintf("%v", l.confValue)
	case ValueEntry:
		v = fmt.Sprintf("%d", l.normalValue)
	default:
		panic(fmt.Sprintf("unknown entry type %s", l.Type))
	}

	return fmt.Sprintf("{ term: %d, type: %s, value: %s }",
		l.Term, l.Type.String(), v)
}

type msg struct {
	Type MessageType

	// Null if Type != AppendEntriesReq
	Entries []lentry
}

type event struct {
	Type      EventType
	Message   msg
	Sender    int
	Recipient int
}

func ParseFile(fname string) ([]event, error) {
	f, err := os.Open(fname)

	defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s", fname)
	}
	r := []event{}
	d := json.NewDecoder(f)
	for {
		var v event
		if err := d.Decode(&v); err == io.EOF {
			break // done decoding file
		} else if err != nil {
			// handle error
		}
		r = append(r, v)
	}
	return r, nil
}

// These mirror the types in the TLA+ spec

type tmsg struct {
	Mtype    string   `json:"mtype"`
	Msource  string   `json:"msource"`
	Mdest    string   `json:"mdest"`
	Mentries []lentry `json:"mentries"`
	Mterm    int      `json:"mterm"`
}

type Trace struct {
	State struct {
		History struct {
			Global []struct {
				Action     string `json:"action"`
				ExecutedOn string `json:"executedOn"`
				Removed    string `json:"removed"`
				Msg        tmsg   `json:"msg"`
			} `json:"global"`
			HadNumLeaders int `json:"hadNumLeaders"`
		} `json:"history"`
		Log map[string][]lentry `json:"log"`
	} `json:"state"`
}

// Convert to the more abstract representation of messages
// the interpreter uses
func convertMsg(m tmsg) msg {
	switch m.Mtype {
	case "RequestVoteRequest":
		return msg{Type: RequestVoteReq}
	case "RequestVoteResponse":
		return msg{Type: RequestVoteRes}
	case "AppendEntriesRequest":
		return msg{Type: AppendEntriesReq, Entries: convertLEntries((m.Mentries))}
	case "AppendEntriesResponse":
		return msg{Type: AppendEntriesRes}
	default:
		panic("unimplemented message type " + m.Mtype)
	}
}

func preprocessEvents(events []event) []event {
	res := []event{}
	for _, e := range events {
		if e.Sender == e.Recipient {
			// do nothing; ignore self-sends as they don't go through the transport
			fmt.Printf("Ignored event %+v\n", e)
		} else {
			res = append(res, e)
		}
	}
	return res
}

func parseServerId(name string) int {
	id, err := strconv.Atoi(name[1:])
	if err != nil {
		log.Fatalf("invalid server name %s", name)
	}
	return id
}

func ParseTrace(fname string) ([]Trace, []event) {
	f, err := os.Open(fname)
	defer f.Close()
	if err != nil {
		log.Fatalf("failed to open file %s", fname)
	}
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("failed to read file %s", fname)
	}
	var trace []Trace
	err1 := json.Unmarshal(bytes, &trace)
	if err1 != nil {
		log.Fatal(err1)
	}
	if len(trace) == 0 {
		log.Fatalf("nonempty trace required")
	}
	global := trace[len(trace)-1].State.History.Global
	res := []event{}
	for _, v := range global {
		if v.Action == "Timeout" {
			res = append(res, event{Type: Timeout, Recipient: parseServerId(v.ExecutedOn)})
		} else if v.Action == "Send" {
			if v.Msg.Mtype != "CheckOldConfig" {
				res = append(res, event{Type: Send,
					Message:   convertMsg(v.Msg),
					Sender:    parseServerId(v.Msg.Msource),
					Recipient: parseServerId(v.Msg.Mdest),
				})
			}
		} else if v.Action == "Receive" {
			if v.Msg.Mtype != "CheckOldConfig" {
				res = append(res, event{Type: Receive,
					Message:   convertMsg(v.Msg),
					Sender:    parseServerId(v.Msg.Msource),
					Recipient: parseServerId(v.Msg.Mdest),
				})
			}
		} else if v.Action == "BecomeLeader" {
			res = append(res, event{Type: BecomeLeader,
				Recipient: parseServerId(v.ExecutedOn),
			})
		} else if v.Action == "CommitEntry" {
			// do nothing. this is implicit in the implementation
		} else if v.Action == "Restart" {
			res = append(res, event{Type: Restart,
				Recipient: parseServerId(v.ExecutedOn),
			})
		} else if v.Action == "TryRemoveServer" {
			res = append(res, event{Type: TryRemoveServer,
				// Overload this field
				Sender:    parseServerId(v.Removed),
				Recipient: parseServerId(v.ExecutedOn),
			})
		} else if v.Action == "RemoveServer" {
			res = append(res, event{Type: RemoveServer,
				Recipient: parseServerId(v.Removed),
			})
		} else {
			log.Fatalf("unimplemented action %+v", v)
		}
	}
	return trace, preprocessEvents(res)
}

func convertLEntry(entry lentry) lentry {
	switch entry.Type {
	case ConfigEntry:
		json.Unmarshal(entry.Value, &entry.confValue)
		return entry
	case ValueEntry:
		json.Unmarshal(entry.Value, &entry.normalValue)
		return entry
	default:
		panic(fmt.Sprintf("unknown entry type %s", entry.Type))
	}
}

func convertLEntries(entries []lentry) []lentry {
	r := []lentry{}
	for _, e := range entries {
		r = append(r, convertLEntry(e))
	}
	return r
}

func convertAbsLog(log map[string][]lentry) map[int][]lentry {
	r := make(map[int][]lentry)
	for id, v := range log {
		r[parseServerId(id)] = convertLEntries(v)
	}
	return r
}

// This should be comparable via deep equality
type absState struct {
	atLeastOneLeader bool
	logs             map[int][]lentry
}

func (s absState) String() string {
	keys := make([]int, 0, len(s.logs))
	for k := range s.logs {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	m := []string{}
	for _, k := range keys {
		m = append(m, fmt.Sprintf("%d: %s", k, s.logs[k]))
	}
	slog := fmt.Sprintf("{ %s }", strings.Join(m, ", "))
	return fmt.Sprintf("{ atLeastOneLeader: %t, logs: %s }", s.atLeastOneLeader, slog)
}

func abstractEntryType(t pb.EntryType) EntryType {
	switch t {
	case pb.EntryConfChange:
		return ConfigEntry
	case pb.EntryConfChangeV2:
		panic("conf change v2 not yet implemented")
	case pb.EntryNormal:
		return ValueEntry
	default:
		panic(fmt.Sprintf("unknown entry type %s", t))
	}
}

func abstractEntry(entries []pb.Entry) []lentry {
	r := []lentry{}
	confNodes := []int{}
	for _, e := range entries {

		// truncation is okay because model checking won't produce long logs
		term := int(e.Term)
		typ := abstractEntryType(e.Type)

		switch e.Type {
		case pb.EntryNormal:
			var val entryValue
			if len(e.Data) == 0 {
				val = 0
			} else {
				val = deserializeValue(e.Data)
			}
			r = append(r, lentry{
				Term:        term,
				Type:        typ,
				normalValue: val})
		case pb.EntryConfChange:

			// Determine the current conf from the change operations in the log
			var cc raftpb.ConfChange
			cc.Unmarshal(e.Data)
			switch cc.Type {
			case pb.ConfChangeAddNode:
				// truncation okay because we won't have a ton of nodes
				confNodes = append(confNodes, int(cc.NodeID))
			case pb.ConfChangeRemoveNode:
				cn := []int{}
				for _, v := range confNodes {
					if v != int(cc.NodeID) { // truncation ok
						cn = append(cn, v)
					}
				}
				confNodes = cn
			default:
				log.Fatalf("unimplemented conf change type %s", cc.Type)
			}
			currentConf := make([]int, len(confNodes))
			copy(currentConf, confNodes)

			r = append(r, lentry{
				Term:      term,
				Type:      typ,
				confValue: currentConf})
		case pb.EntryConfChangeV2:
			panic("conf change v2 unimplemented")
		default:
			panic(fmt.Sprintf("unknown entry type %s", e.Type))
		}
	}
	return r
}

// Get rid of spurious conf changes in prefix of log, as these appear in the implementation only, not the spec
func removeInitialConfChanges(entries []lentry) []lentry {
	r := []lentry{}
	prefix := true
	for _, v := range entries {
		if prefix {
			if v.Type == ConfigEntry {
				continue
			} else {
				prefix = false
				r = append(r, v)
			}
		} else {
			r = append(r, v)
		}
	}
	return r
}

func abstractEntries(nodes map[int]*raftNode) map[int][]lentry {
	r := make(map[int][]lentry)
	for id, n := range nodes {
		// TODO there's a null pointer here sometimes
		r[id] = removeInitialConfChanges(abstractEntry(n.node.Raft().Log().Entries()))
	}
	return r
}

func (itp *interpreter) abstract() absState {
	return absState{
		atLeastOneLeader: true,
		logs:             abstractEntries(itp.nodes),
	}
}
