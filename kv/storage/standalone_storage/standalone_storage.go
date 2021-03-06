package standalone_storage

import (
	"errors"
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		engine: &engine_util.Engines{},
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dbPath := s.config.DBPath
	kvPath := path.Join(dbPath, "kv")
	raftPath := path.Join(dbPath, "raft")

	kvEngine := engine_util.CreateDB(kvPath, false)
	raftEngine := engine_util.CreateDB(raftPath, true)

	s.engine = engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)

	if s.engine != nil {
		return nil
	}
	return errors.New("failed to start engine!! ")
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

//Reader function
//kvrpcpb.Context is a miscellaneous data present in request
//storage.StorageReader is a interface,so need to implement it
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {

	// Your Code Here (1).

	return NewStandAloneReader(s.engine.Kv.NewTransaction(false)), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			err := engine_util.PutCF(s.engine.Kv, put.Cf, put.Key, put.Value)
			if nil != err {
				return err
			}
		case storage.Delete:
			del := b.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engine.Kv, del.Cf, del.Key)
			if nil != err {
				return err
			}
		}
	}
	// Your Code Here (1).

	return nil
}

type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneReader(kvTxn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		kvTxn: kvTxn,
	}
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.kvTxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err

}
func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.kvTxn)
}
func (s *StandAloneReader) Close() {
	s.kvTxn.Discard()
}
