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

	raft *raft.Raft
}

// New returns a new Store.
func New(storagePath string, raftAddress string, singleMode bool) *Store {
	return &Store{storagePath: storagePath, raftAddress: raftAddress, singleMode: singleMode}
}

func (s *Store) Open() error {
	config := raft.DefaultConfig()
	config.SnapshotInterval = time.Hour
	config.SnapshotThreshold = 10000000

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

	return nil
}

func (s *Store) Count() int {
	storageUnique.Lock()
	storageUnique.Unlock()
	return len(storageUnique.m)
}

func (s *Store) CheckNSet(key string, value string, expiration time.Time) (bool, string, error) {
	if s.raft.State() != raft.Leader {
		return false, "", fmt.Errorf("not leader")
	}

	msg := &MessageValue{
		Operation:  MessageValue_CAS,
		Key:        key,
		Value:      value,
		Expiration: uint32(expiration.Unix()),
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return false, "", err
	}

	f := s.raft.Apply(b, raftTimeout)
	if f.Error() != nil {
		return false, "", f.Error()
	}
	r := f.Response().(*fsmResponse)
	return r.exists, r.value, r.error
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
	exists bool
	value  string
	error  error
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	msg := &MessageValue{}
	response := &fsmResponse{}

	if err := json.Unmarshal(l.Data, msg); err != nil {
		return &fsmResponse{error: fmt.Errorf("failed to unmarshal command: %s", err.Error())}
	}

	if msg.Operation == MessageValue_CAS {
		storageUnique.Lock()
		storageUnique.Unlock()
		storageValue, ok := storageUnique.m[msg.Key]
		if !ok {
			if msg.Expiration > uint32(time.Now().Unix()) {
				val := &UniqueStorageValue{Expiration: msg.Expiration, Value: msg.Value}
				storageUnique.m[msg.Key] = val
				response.value = msg.Value
			}
		} else {
			expiration := time.Unix(int64(msg.Expiration), 0)
			if expiration.After(time.Now()) {
				response.exists = true
				response.value = storageValue.Value
			}
		}
	}
	return response
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	result := &UniqueStorage{}
	result.Items = make(map[string]*UniqueStorageValue)

	storageUnique.Lock()
	defer storageUnique.Unlock()

	for k, item := range storageUnique.m {
		result.Items[k] = &UniqueStorageValue{Expiration: item.Expiration, Value: item.Value}
	}

	return result, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	log.Printf("Restore running\n")

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
	fmt.Printf("Persist running")
	err := func() error {
		b, err := json.Marshal(f)
		if err != nil {
			log.Printf("Error in Persist Marshal %v\n", err)
			return err
		}

		if _, err := sink.Write(b); err != nil {
			log.Printf("Error in Persist Write %v\n", err)
			return err
		}

		if err := sink.Close(); err != nil {
			log.Printf("Error in Persist Close %v\n", err)
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
