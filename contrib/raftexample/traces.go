package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
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
)

func (s MessageType) String() string {
	return rvToString[s]
}

var rvToString = map[MessageType]string{
	RequestVoteReq: "RequestVoteReq",
	RequestVoteRes: "RequestVoteRes",
}

var rvToID = map[string]MessageType{
	"RequestVoteReq": RequestVoteReq,
	"RequestVoteRes": RequestVoteRes,
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

type Trace struct {
	State struct {
		History struct {
			Global []struct {
				Action     string `json:"action"`
				ExecutedOn string `json:"executedOn"`
				Msg        tmsg   `json:"msg"`
			} `json:"global"`
			HadAtLeastOneLeader bool `json:"hadAtLeastOneLeader"`
		} `json:"history"`
	} `json:"state"`
}

func convertMsg(m tmsg) msg {
	if m.Mtype == "RequestVoteRequest" {
		return msg{Type: RequestVoteReq}
	} else if m.Mtype == "RequestVoteResponse" {
		return msg{Type: RequestVoteRes}
	} else {
		panic("unimplemented message type " + m.Mtype)
	}
}

func preprocessEvents(events []event) []event {
	res := []event{}
	for _, e := range events {
		if e.Sender == e.Recipient {
			// do nothing; ignore self-sends as they don't go through the transport
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

func ParseLog(fname string) ([]Trace, []event) {
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
