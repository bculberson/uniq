package diskStore

import (
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

type Store struct {
	storagePath string
	raftAddress string
	singleMode  bool

	pendingMsgs chan MessageValue
	pendingData []MessageValue
	uniqStore   *bolt.DB
	raft        *raft.Raft
}

type fsm Store

type fsmKeyResponse struct {
	exists bool
	value  string
	error  error
}

type fsmResponse struct {
	data  map[string]*fsmKeyResponse
	error error
}

type MessageValue_OPERATION int32

const (
	MessageValue_CAS MessageValue_OPERATION = 0
)

type MessageValue struct {
	Operation  MessageValue_OPERATION `json:"operation,omitempty"`
	Key        string                 `json:"key,omitempty"`
	Value      string                 `json:"value,omitempty"`
	Expiration int64                  `json:"expiration,omitempty"`
	callback   chan *fsmResponse
}

type UniqueStorage struct {
	Items map[string]*UniqueStorageValue `json:"items,omitempty"`
}

type UniqueStorageValue struct {
	Value      string `json:"value,omitempty"`
	Expiration int64 `json:"expiration,omitempty"`
}

