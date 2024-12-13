package resumable

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/rs/zerolog/log"
	bolt "go.etcd.io/bbolt"
	"path"
)

var bucketName = []byte("clickhouse-backup")

type State struct {
	stateFile string
	db        *bolt.DB
	params    map[string]interface{}
}

func NewState(stateBackupDir, backupName, command string, params map[string]interface{}) *State {
	s := State{
		stateFile: path.Join(stateBackupDir, "backup", backupName, fmt.Sprintf("%s.state2", command)),
		db:        nil,
	}
	if db, err := bolt.Open(s.stateFile, 0600, nil); err == nil {
		s.db = db
	} else {
		log.Warn().Msgf("resumable state: can't open %s error: %v", s.stateFile, err)
		return &s
	}
	s.LoadState()
	s.LoadParams()
	s.CleanupStateIfParamsChange(params)
	return &s
}

func (s *State) GetParams() map[string]interface{} {
	return s.params
}

func (s *State) getBucket(tx *bolt.Tx) *bolt.Bucket {
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		log.Fatal().Msgf("resumable state: can't open bucket %s in %s", bucketName, s.stateFile)
	}
	return bucket
}

func (s *State) LoadParams() {
	if s.db == nil {
		return
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		bucket := s.getBucket(tx)
		params := bucket.Get([]byte("params"))
		if params != nil {
			s.params = make(map[string]interface{})
			return json.Unmarshal(params, &s.params)
		}
		return nil
	}); err != nil {
		log.Warn().Msgf("resumable state: can't load params from %s, error: %v", s.stateFile, err)
	}
}

func (s *State) LoadState() {
	if s.db == nil {
		return
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			bucket, err = tx.CreateBucket(bucketName)
			if err != nil {
				return fmt.Errorf("resumable state: can't create bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		log.Warn().Msgf("LoadState error: %v", err)
	}
}

func (s *State) CleanupStateIfParamsChange(params map[string]interface{}) {
	if s.db == nil {
		return
	}
	needCleanup := false
	if s.params != nil && params == nil {
		needCleanup = true
	}
	if s.params != nil && params != nil && !common.CompareMaps(s.params, params) {
		needCleanup = true
	}

	if needCleanup {
		log.Info().Msgf("parameters changed old=%#v new=%#v, %s cleanup begin", s.params, params, s.stateFile)
		err := s.db.Batch(func(tx *bolt.Tx) error {
			b := s.getBucket(tx)
			c := b.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if err := b.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			log.Warn().Msgf("resumable state: can't cleanupBucket %s in %s", bucketName, s.stateFile)
		}
	}
	_ = s.db.Batch(func(tx *bolt.Tx) error {
		b := s.getBucket(tx)
		s.saveParams(b, params)
		return nil
	})

}

func (s *State) saveParams(b *bolt.Bucket, params map[string]interface{}) {
	if params != nil {
		s.params = params
	}
	if s.params == nil {
		return
	}
	paramsBytes, err := json.Marshal(s.params)
	if err != nil {
		log.Warn().Msgf("resumable state: can't json.Marshal(s.params=%#v): %v", s.params, err)
		return
	}
	if err = b.Put([]byte("params"), paramsBytes); err != nil {
		log.Warn().Msgf("resumable state: can't bolt.Put(s.params): %v", err)
	}
}

func (s *State) AppendToState(path string, size int64) {
	if s.db == nil {
		return
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := s.getBucket(tx)
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutVarint(buf, size)
		return b.Put([]byte(path), buf[:n])
	})
	if err != nil {
		log.Fatal().Msgf("resumable state: can't write key %s to %s error: %v", path, s.stateFile, err)
	}
}

func (s *State) IsAlreadyProcessedBool(path string) bool {
	isProcesses, _ := s.IsAlreadyProcessed(path)
	return isProcesses
}

func (s *State) IsAlreadyProcessed(path string) (bool, int64) {
	if s.db == nil {
		return false, 0
	}
	size := int64(0)
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		b := s.getBucket(tx)
		buf := b.Get([]byte(path))
		if buf != nil {
			found = true
			n := 0
			size, n = binary.Varint(buf)
			if n == 0 {
				return fmt.Errorf("buffer too small")
			} else if n < 0 {
				return fmt.Errorf("value larger than 64 bits (overflow)")
			}
			log.Info().Msgf("%s already processed, size %s", path, utils.FormatBytes(uint64(size)))
		}
		return nil
	})
	if err != nil {
		log.Fatal().Msgf("resumable state: can't read key %s to %s error: %v", path, s.stateFile, err)
		return false, 0
	}
	return found, size
}

func (s *State) Close() {
	if s.db == nil {
		return
	}
	if err := s.db.Close(); err != nil {
		log.Warn().Err(err).Msgf("resumable state: can't close %s", s.stateFile)
	}
}
