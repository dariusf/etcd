package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
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
	Msource int    `json:"msource"`
	Mdest   int    `json:"mdest"`
}

type Trace struct {
	State struct {
		History struct {
			Global []struct {
				Action     string `json:"action"`
				ExecutedOn int    `json:"executedOn"`
				Msg        tmsg   `json:"msg"`
			} `json:"global"`
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

func ParseLog(fname string) []event {
	f, err := os.Open(fname)
	defer f.Close()
	if err != nil {
		panic("failed to open file")
	}
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		panic("failed to read file")
	}
	var trace []Trace
	err1 := json.Unmarshal(bytes, &trace)
	if err1 != nil {
		panic("failed to parse json")
	}
	global := trace[len(trace)-1].State.History.Global
	res := []event{}
	for _, v := range global {
		if v.Action == "Timeout" {
			res = append(res, event{Type: Timeout, Recipient: v.ExecutedOn})
		} else if v.Action == "Send" {
			res = append(res, event{Type: Send,
				Message:   convertMsg(v.Msg),
				Sender:    v.Msg.Msource,
				Recipient: v.Msg.Mdest,
			})
		} else if v.Action == "Receive" {
			res = append(res, event{Type: Receive,
				Message:   convertMsg(v.Msg),
				Sender:    v.Msg.Msource,
				Recipient: v.Msg.Mdest,
			})
		} else if v.Action == "BecomeLeader" {
			panic(fmt.Sprintf("%#v", v) + " should not appear in traces")
		} else {
			panic("unimplemented action " + fmt.Sprintf("%#v", v))
		}
	}
	return preprocessEvents(res)
}
