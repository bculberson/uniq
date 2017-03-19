package store

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
	"encoding/json"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	flushDuration       = 100 * time.Millisecond
	flushMaxMessages    = 1000
	snapshotInterval    = time.Hour
	snapshotThreshold   = 10000000
)

var (
	logger        = log.New(os.Stderr, "[store] ", log.LstdFlags)
	storageUnique = struct {
		sync.RWMutex
		m map[string]*UniqueStorageValue
	}{
		m: make(map[string]*UniqueStorageValue),
	}
)

type Store struct {
	storagePath string
	raftAddress string
	singleMode  bool
	currentLdr  string

	raft        *raft.Raft
	pendingCh   chan *MessageValue
	pendingData []*MessageValue
}

// New returns a new Store.
func New(storagePath string, raftAddress string, singleMode bool) *Store {
	return &Store{storagePath: storagePath, raftAddress: raftAddress, singleMode: singleMode, pendingCh: make(chan *MessageValue), pendingData: make([]*MessageValue,0)}
}

func (s *Store) flush() {
	b, err := json.Marshal(s.pendingData)
	if err != nil {
		logger.Printf("Error marshaling data %v", err)
		s.pendingData = make([]*MessageValue, 0)
		return
	}

	f := s.raft.Apply(b, raftTimeout)
	if f.Error() != nil {
		logger.Printf("Error applying data %v", f.Error())
		return
	}
	s.pendingData = make([]*MessageValue, 0)
}

func (s *Store) Open() error {
	config := raft.DefaultConfig()
	config.SnapshotInterval = snapshotInterval
	config.SnapshotThreshold = snapshotThreshold

	addr, err := net.ResolveTCPAddr("tcp", s.raftAddress)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.raftAddress, addr, 3, raftTimeout, os.Stderr)
	if err != nil {
		return err
	}

	peerStore := raft.NewJSONPeers(s.storagePath, transport)

	peers, err := peerStore.Peers()
	if err != nil {
		return err
	}

	if s.singleMode && len(peers) <= 1 {
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	snapshots, err := raft.NewFileSnapshotStore(s.storagePath, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.storagePath, "raft.log.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, logStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	go func() {
		ticker := time.NewTicker(flushDuration)
		for {
			select {
			case <-ticker.C:
				if len(s.pendingData) > 0 {
					s.flush()
				}
			case mv := <-s.pendingCh:
				s.pendingData = append(s.pendingData, mv)
				if len (s.pendingData) > flushMaxMessages {
					s.flush()
				}
			}
		}
	}()

	return nil
}

func (s *Store) Count() int {
	storageUnique.RLock()
	storageUnique.RUnlock()
	return len(storageUnique.m)
}

func (s *Store) CheckNSet(key string, value string, expiration time.Time) (bool, string, error) {
	if s.raft.State() != raft.Leader {
		return false, "", fmt.Errorf("not leader")
	}

	var exists bool
	storageUnique.Lock()
	storageValue, found := storageUnique.m[key]
	if found {
		expiration := time.Unix(int64(storageValue.Expiration), 0)
		if expiration.After(time.Now()) {
			value = storageValue.Value
			exists = true
		}
	}
	if !exists {
		val := &UniqueStorageValue{Expiration: uint32(expiration.Unix()), Value: value}
		storageUnique.m[key] = val
	}
	storageUnique.Unlock()

	if !exists {
		msg := &MessageValue{
			Operation:  MessageValue_CAS,
			Key:        key,
			Value:      value,
			Expiration: uint32(expiration.Unix()),
		}

		s.pendingCh <- msg
	}

	return exists, value, nil
}

func (s *Store) Join(addr string) error {
	f := s.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	return nil
}

func (s *Store) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

type fsm Store

type fsmResponse struct {
	error error
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	msgs := make([]*MessageValue, 0)
	response := &fsmResponse{}

	if err := json.Unmarshal(l.Data, msgs); err != nil {
		return &fsmResponse{error: fmt.Errorf("failed to unmarshal command: %s", err.Error())}
	}

	storageUnique.Lock()
	for _, msg := range msgs {
		if msg.Operation == MessageValue_CAS {
			val := &UniqueStorageValue{Expiration: msg.Expiration, Value: msg.Value}
			storageUnique.m[msg.Key] = val
		}
	}
	storageUnique.Unlock()
	return response
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	result := &UniqueStorage{}
	result.Items = make(map[string]*UniqueStorageValue)

	storageUnique.RLock()
	defer storageUnique.RUnlock()

	for k, item := range storageUnique.m {
		result.Items[k] = &UniqueStorageValue{Expiration: item.Expiration, Value: item.Value}
	}

	return result, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	logger.Printf("Restore running\n")

	data := &UniqueStorage{}
	bytes, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(bytes, data); err != nil {
		return err
	}

	storageUnique.Lock()
	defer storageUnique.Unlock()

	storageUnique.m = make(map[string]*UniqueStorageValue)
	for k, storageValue := range data.Items {
		exp := time.Unix(int64(storageValue.Expiration), 0)
		if exp.After(time.Now()) {
			storageUnique.m[k] = storageValue
		}
	}
	return err
}

func (f *UniqueStorage) Persist(sink raft.SnapshotSink) error {
	logger.Printf("Persist running")
	err := func() error {
		b, err := json.Marshal(f)
		if err != nil {
			logger.Printf("Error in Persist Marshal %v\n", err)
			return err
		}

		if _, err := sink.Write(b); err != nil {
			logger.Printf("Error in Persist Write %v\n", err)
			return err
		}

		if err := sink.Close(); err != nil {
			logger.Printf("Error in Persist Close %v\n", err)
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *UniqueStorage) Release() {}
