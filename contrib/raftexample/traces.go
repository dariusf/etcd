package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type EventType int

const (
	Timeout EventType = iota
	Send
	Receive
	BecomeLeader
)

func (s EventType) String() string {
	return toString[s]
}

var toString = map[EventType]string{
	Timeout:      "Timeout",
	Send:         "Send",
	Receive:      "Receive",
	BecomeLeader: "BecomeLeader",
}

var toID = map[string]EventType{
	"Timeout":      Timeout,
	"Send":         Send,
	"Receive":      Receive,
	"BecomeLeader": BecomeLeader,
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

type msg struct {
	Type MessageType
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

type tmsg struct {
	Mtype   string `json:"mtype"`
	Msource string `json:"msource"`
	Mdest   string `json:"mdest"`
}

type lentry struct {
	Term        int             `json:"term"`
	Type        EntryType       `json:"type"`
	Value       json.RawMessage `json:"value"`
	normalValue int
	confValue   []int
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

type Trace struct {
	State struct {
		History struct {
			Global []struct {
				Action     string `json:"action"`
				ExecutedOn string `json:"executedOn"`
				Msg        tmsg   `json:"msg"`
			} `json:"global"`
			HadNumLeaders int `json:"hadNumLeaders"`
		} `json:"history"`
		Log map[string][]lentry `json:"log"`
	} `json:"state"`
}

func convertMsg(m tmsg) msg {
	switch m.Mtype {
	case "RequestVoteRequest":
		return msg{Type: RequestVoteReq}
	case "RequestVoteResponse":
		return msg{Type: RequestVoteRes}
	case "AppendEntriesRequest":
		return msg{Type: AppendEntriesReq}
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
	global := trace[len(trace)-1].State.History.Global
	res := []event{}
	for _, v := range global {
		if v.Action == "Timeout" {
			res = append(res, event{Type: Timeout, Recipient: parseServerId(v.ExecutedOn)})
		} else if v.Action == "Send" {
			res = append(res, event{Type: Send,
				Message:   convertMsg(v.Msg),
				Sender:    parseServerId(v.Msg.Msource),
				Recipient: parseServerId(v.Msg.Mdest),
			})
		} else if v.Action == "Receive" {
			res = append(res, event{Type: Receive,
				Message:   convertMsg(v.Msg),
				Sender:    parseServerId(v.Msg.Msource),
				Recipient: parseServerId(v.Msg.Mdest),
			})
		} else if v.Action == "BecomeLeader" {
			res = append(res, event{Type: BecomeLeader,
				Recipient: parseServerId(v.ExecutedOn),
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

func convertAbsLog(log map[string][]lentry) map[int][]lentry {
	r := make(map[int][]lentry)
	for id, v := range log {
		r1 := []lentry{}
		for _, e := range v {
			r1 = append(r1, convertLEntry(e))
		}
		r[parseServerId(id)] = r1
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

func abstractEntry(log []pb.Entry) []lentry {
	r := []lentry{}
	for _, v := range log {
		// TODO remove the initial conf change entries

		// truncation is okay because the spec won't have extremely long logs
		term := int(v.Term)
		typ := abstractEntryType(v.Type)
		switch v.Type {
		case pb.EntryNormal:
			r = append(r, lentry{
				Term:        term,
				Type:        typ,
				normalValue: 0}) // TODO
		case pb.EntryConfChange:
			r = append(r, lentry{
				Term:      term,
				Type:      typ,
				confValue: nil}) // TODO
		case pb.EntryConfChangeV2:
			panic("conf change v2 unimplemented")
		default:
			panic(fmt.Sprintf("unknown entry type %s", v.Type))
		}
	}
	return r
}

func abstractEntries(nodes map[int]*raftNode) map[int][]lentry {
	r := make(map[int][]lentry)
	for id, n := range nodes {
		r[id] = abstractEntry(n.node.Raft().Log().Entries())
	}
	return r
}

func abstract(transport *Transport, nodes map[int]*raftNode) absState {
	return absState{
		atLeastOneLeader: true,
		logs:             abstractEntries(nodes),
	}
}
