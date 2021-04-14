package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
)

type EventType int

const (
	Timeout EventType = iota
	Send
	Receive
)

func (s EventType) String() string {
	return toString[s]
}

var toString = map[EventType]string{
	Timeout: "Timeout",
	Send:    "Send",
	Receive: "Receive",
}

var toID = map[string]EventType{
	"Timeout": Timeout,
	"Send":    Send,
	"Receive": Receive,
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
)

func (s MessageType) String() string {
	return rvToString[s]
}

var rvToString = map[MessageType]string{
	RequestVoteReq: "RequestVoteReq",
}

var rvToID = map[string]MessageType{
	"RequestVoteReq": RequestVoteReq,
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

func parseFile(fname string) []event {
	f, err := os.Open(fname)
	if err != nil {
		// handle error
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
	return r
}
