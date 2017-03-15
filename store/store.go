package store

import (
	"encoding/json"
	"fmt"
	"time"
	"os"
	"log"
	"net"
	"io"
	"path/filepath"
	"strconv"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

var logger = log.New(os.Stderr, "[store] ", log.LstdFlags)

type command struct {
	Op         string `json:"op,omitempty"`
	Key        string `json:"key,omitempty"`
	Value      string `json:"value,omitempty"`
	Expiration string `json:"expiration,omitempty"`
}

type Store struct {
	storagePath string
	raftAddress string
	singleMode  bool
	currentLdr  string

	uniqStore *bolt.DB
	raft      *raft.Raft
}

// New returns a new Store.
func New(storagePath string, raftAddress string, singleMode bool) *Store {
	return &Store{storagePath: storagePath, raftAddress: raftAddress, singleMode: singleMode}
}

func (s *Store) Open() error {
	keyStore, err := bolt.Open(filepath.Join(s.storagePath, "raft.uniques.db"), 0644, nil)
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}
	err = keyStore.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("uniques"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	s.uniqStore = keyStore

	config := raft.DefaultConfig()

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

func (s *Store) CheckNSet(key string, value string, expiration time.Time) (bool, string, error) {
	if s.raft.State() != raft.Leader {
		return false, "", fmt.Errorf("not leader")
	}

	c := &command{
		Op:         "cns",
		Key:        key,
		Value:      value,
		Expiration: strconv.FormatInt(expiration.Unix(), 10),
	}
	b, err := json.Marshal(c)
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

type cnsStoreValue struct {
	Value      string `json:"value,omitempty"`
	Expiration int64  `json:"exp,omitempty"`
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	response := &fsmResponse{}

	if err := json.Unmarshal(l.Data, &c); err != nil {
		return &fsmResponse{error: fmt.Errorf("failed to unmarshal command: %s", err.Error())}
	}

	tx, err := f.uniqStore.Begin(true)
	if err != nil {
		response.error = err
		return response
	}
	defer tx.Rollback()

	if c.Op == "cns" {
		b := tx.Bucket([]byte("uniques"))
		v := b.Get([]byte(c.Key))
		if v == nil {
			unixTime, _ := strconv.ParseInt(c.Expiration, 10, 64)
			expiration := time.Unix(unixTime, 0)
			if expiration.After(time.Now()) {
				log.Printf("Adding %s key to db with value %s", c.Key, c.Value)
				storageValue := &cnsStoreValue{Expiration: unixTime, Value: c.Value}
				storageValueInBytes, err := json.Marshal(storageValue)
				if err != nil {
					response.error = err
					return response
				}
				b.Put([]byte(c.Key), storageValueInBytes)
				response.value = c.Value
			}
		} else {
			var storageValue cnsStoreValue
			if err := json.Unmarshal(v, &storageValue); err != nil {
				response.error = fmt.Errorf("failed to unmarshal data: %s", err.Error())
				return response
			}

			exp := time.Unix(storageValue.Expiration, 0)
			if exp.After(time.Now()) {
				log.Printf("Key %s exists, date %v after %v", c.Key, exp, time.Now())
				response.exists = true
				response.value = storageValue.Value
			} else {
				log.Printf("Key %s exists, date %v before %v", c.Key, exp, time.Now())
				b.Delete([]byte(c.Key))
			}
		}
	}
	if err := tx.Commit(); err != nil {
		response.error = err
		return response
	}
	return response
}

type fsmSnapshot struct {
	Store map[string]cnsStoreValue
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	log.Printf("Snapshot running")
	result := &fsmSnapshot{}
	result.Store = make(map[string]cnsStoreValue)
	err := f.uniqStore.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("uniques"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var storageValue cnsStoreValue
			if err := json.Unmarshal(v, &storageValue); err != nil {
				log.Printf("Error in Snapshot %v\n", err)
				return err
			}

			exp := time.Unix(storageValue.Expiration, 0)
			if exp.After(time.Now()) {
				key := string(k)
				result.Store[key] = storageValue
			}
		}
		return nil
	})
	return result, err
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	fmt.Printf("Restore running")
	data := make(map[string]cnsStoreValue)
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}
	err := f.uniqStore.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("uniques"))
		for k, storageValue := range data {
			exp := time.Unix(storageValue.Expiration, 0)
			if exp.After(time.Now()) {
				storageValueInBytes, err := json.Marshal(storageValue)
				if err != nil {
					log.Printf("Error in Restore %v\n", err)
					return err
				}
				b.Put([]byte(k), storageValueInBytes)
			}
		}
		return nil
	})
	return err
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	fmt.Printf("Persist running")
	err := func() error {
		b, err := json.Marshal(f.Store)
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

func (f *fsmSnapshot) Release() {}
