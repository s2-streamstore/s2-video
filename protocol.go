package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	MsgTypeVideo   byte = 0x01
	MsgTypeAudio   byte = 0x02
	MsgTypeControl byte = 0x03
	MsgTypeChat    byte = 0x04
	MsgTypeScreen  byte = 0x05
	MsgTypeStats   byte = 0x06
)

const (
	ControlJoin           = "join"
	ControlLeave          = "leave"
	ControlParticipants   = "participants"
	ControlError          = "error"
	ControlReplayTime     = "replay_time"
	ControlReplayComplete = "replay_complete"
)

type ControlMessage struct {
	Type         string   `json:"type"`
	UserID       string   `json:"user_id,omitempty"`
	Participants []string `json:"participants,omitempty"`
	Message      string   `json:"message,omitempty"`
	Timestamp    int64    `json:"timestamp,omitempty"`
}

// [1 byte type][2 byte userID length][N bytes userID][payload]
func EncodeMediaMessage(msgType byte, userID string, payload []byte) []byte {
	userIDBytes := []byte(userID)
	msg := make([]byte, 1+2+len(userIDBytes)+len(payload))
	msg[0] = msgType
	binary.BigEndian.PutUint16(msg[1:3], uint16(len(userIDBytes)))
	copy(msg[3:3+len(userIDBytes)], userIDBytes)
	copy(msg[3+len(userIDBytes):], payload)
	return msg
}

// Client sends: [1 byte type][payload]
func DecodeClientMessage(data []byte) (msgType byte, payload []byte, err error) {
	if len(data) < 1 {
		return 0, nil, fmt.Errorf("message too short")
	}
	return data[0], data[1:], nil
}

func EncodeControlMessage(ctrl ControlMessage) ([]byte, error) {
	jsonBytes, err := json.Marshal(ctrl)
	if err != nil {
		return nil, err
	}
	msg := make([]byte, 1+len(jsonBytes))
	msg[0] = MsgTypeControl
	copy(msg[1:], jsonBytes)
	return msg, nil
}
