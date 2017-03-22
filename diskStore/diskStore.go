package diskStore

import (
	"time"
	"github.com/hashicorp/raft"
	"github.com/boltdb/bolt"
	"path/filepath"
	"fmt"
	"github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"io"
	"io/ioutil"
	"encoding/json"
	"log"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	flushDuration       = 10 * time.Millisecond
	flushMaxMessages    = 1000
	snapshotInterval    = time.Hour
	snapshotThreshold   = 10000000
)

var (
	logger        = log.New(os.Stderr, "[diskStore] ", log.LstdFlags)
)

// New returns a new Store.
func New(storagePath string, raftAddress string, singleMode bool) *Store {
	return &Store{storagePath: storagePath, raftAddress: raftAddress, singleMode: singleMode, pendingMsgs: make(chan MessageValue)}
}

func (s *Store) flush() {
	response := &fsmResponse{}
	b, err := json.Marshal(s.pendingData)
	if err != nil {
		response.error = err
	} else {
		f := s.raft.Apply(b, raftTimeout)
		if f.Error() != nil {
			response.error = f.Error()
		}
		response = f.Response().(*fsmResponse)
	}

	for _, msg := range s.pendingData {
		msg.callback <- response
	}
	s.pendingData = make([]MessageValue, 0)
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
		return fmt.Errorf("file snapshot memoryStore: %s", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.storagePath, "raft.log.db"))
	if err != nil {
		return fmt.Errorf("new bolt memoryStore: %s", err)
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
			case msg := <-s.pendingMsgs:
				s.pendingData = append(s.pendingData, msg)
				if len(s.pendingData) > flushMaxMessages {
					s.flush()
				}
			}
		}
	}()

	return nil
}

func (s *Store) CheckNSet(key string, value string, expiration time.Time) (bool, string, error) {
	if s.raft.State() != raft.Leader {
		return false, "", fmt.Errorf("not leader")
	}

	resultCh := make(chan *fsmResponse)
	msg := MessageValue{
		Operation:  MessageValue_CAS,
		Key:        key,
		Value:      value,
		Expiration: expiration.Unix(),
		callback:   resultCh,
	}

	s.pendingMsgs <- msg
	r := <- msg.callback
	if r.error != nil {
		return false, "", r.error
	}

	return r.data[key].exists, r.data[key].value, r.data[key].error
}

func (s *Store) Count() (int) {
	count := 0
	_ = s.uniqStore.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("uniques"))
		count = b.Stats().KeyN
		return nil
	})
	return count
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

func (f *fsm) Apply(l *raft.Log) interface{} {
	msgs := make([]MessageValue, 0)
	response := &fsmResponse{data: make(map[string]*fsmKeyResponse)}

	if err := json.Unmarshal(l.Data, &msgs); err != nil {
		return &fsmResponse{error: fmt.Errorf("failed to unmarshal command: %s", err.Error())}
	}

	tx, err := f.uniqStore.Begin(true)
	if err != nil {
		response.error = err
		return response
	}
	defer tx.Rollback()

	for _, msg := range msgs {
		if msg.Operation == MessageValue_CAS {
			b := tx.Bucket([]byte("uniques"))
			v := b.Get([]byte(msg.Key))
			response.data[msg.Key] = &fsmKeyResponse{}
			if v == nil {
				expiration := time.Unix(msg.Expiration, 0)
				if expiration.After(time.Now()) {
					storageValue := &UniqueStorageValue{Expiration: msg.Expiration, Value: msg.Value}

					storageValueInBytes, err := json.Marshal(storageValue)
					if err != nil {
						response.data[msg.Key].error = err
					}
					b.Put([]byte(msg.Key), storageValueInBytes)
					response.data[msg.Key].value = msg.Value
				}
			} else {
				storageValue := &UniqueStorageValue{}
				if err := json.Unmarshal(v, storageValue); err != nil {
					response.data[msg.Key].error = fmt.Errorf("failed to unmarshal data: %s", err.Error())
				}

				exp := time.Unix(storageValue.Expiration, 0)
				if exp.After(time.Now()) {
					response.data[msg.Key].exists = true
					response.data[msg.Key].value = storageValue.Value
				} else {
					b.Delete([]byte(msg.Key))
				}
			}
		}
	}
	if err := tx.Commit(); err != nil {
		response.error = err
		return response
	}
	return response
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	log.Printf("Snapshot running")
	result := &UniqueStorage{}
	result.Items = make(map[string]*UniqueStorageValue)

	err := f.uniqStore.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("uniques"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			storageValue := &UniqueStorageValue{}
			if err := json.Unmarshal(v, storageValue); err != nil {
				log.Printf("Error in Snapshot %v\n", err)
				return err
			}

			exp := time.Unix(storageValue.Expiration, 0)
			if exp.After(time.Now()) {
				key := string(k)
				result.Items[key] = storageValue
			}
		}
		return nil
	})
	return result, err
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	fmt.Printf("Restore running")
	data := &UniqueStorage{}
	bytes, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(bytes, data); err != nil {
		return err
	}
	err = f.uniqStore.Update(func(tx *bolt.Tx) error {
		var err error
		var b *bolt.Bucket
		if err = tx.DeleteBucket([]byte("uniques")); err != nil {
			return err
		}
		b, err = tx.CreateBucketIfNotExists([]byte("uniques"))
		if err != nil {
			return err
		}

		for k, storageValue := range data.Items {
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