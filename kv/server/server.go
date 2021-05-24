package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.RawGetResponse{}, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{}, err
	}
	resp := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: false,
	}
	if value == nil {
		resp.NotFound = true
	}
	// Your Code Here (1).
	return resp, err
}

//RawPut
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	batch := storage.Modify{Data: put}

	err := server.storage.Write(req.Context, []storage.Modify{batch})

	if err != nil {
		return &kvrpcpb.RawPutResponse{}, err
	}
	return &kvrpcpb.RawPutResponse{}, err
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	batch := storage.Modify{Data: del}

	err := server.storage.Write(req.Context, []storage.Modify{batch})

	if err != nil {
		return &kvrpcpb.RawDeleteResponse{}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, err
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.RawScanResponse{}, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	iter.Seek(req.StartKey)
	var pairs []*kvrpcpb.KvPair
	limit := req.Limit

	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		val, _ := item.Value()
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		limit--
		if limit == 0 {
			break
		}

	}
	//should close iterator
	resp := &kvrpcpb.RawScanResponse{
		Kvs: pairs,
	}
	return resp, err
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).

	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()

	if nil != err {
		return &kvrpcpb.GetResponse{}, err
	}
	// only use get function
	trac := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := trac.GetLock(req.Key)
	if nil != err {
		return &kvrpcpb.GetResponse{}, err
	}
	if nil != lock && lock.Ts <= req.Version {
		lockInfo := &kvrpcpb.LockInfo{
			PrimaryLock: lock.Primary,
			LockVersion: lock.Ts,
			Key:         req.Key,
			LockTtl:     lock.Ttl,
		}
		return &kvrpcpb.GetResponse{Error: &kvrpcpb.KeyError{Locked: lockInfo}}, nil
	}

	value, err := trac.GetValue(req.Key)
	if nil != err {
		return &kvrpcpb.GetResponse{}, err
	}
	if nil == value {
		return &kvrpcpb.GetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.GetResponse{Value: value}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if nil != err {
		return &kvrpcpb.PrewriteResponse{}, err
	}
	trac := mvcc.NewMvccTxn(reader, req.StartVersion)
	for i := 0; i < len(req.Mutations); i++ {
		mu := req.Mutations[i]
		lock, err := trac.GetLock(mu.Key)
		if nil != err || (nil != lock && lock.Ts != req.StartVersion) {
			lockInfo := &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				LockTtl:     lock.Ttl,
			}
			return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{{Locked: lockInfo}}}, nil
		}
		_, ts, err := trac.MostRecentWrite(mu.Key)
		if nil != err {
			return &kvrpcpb.PrewriteResponse{}, err
		}
		if ts >= req.StartVersion {
			writeConf := &kvrpcpb.WriteConflict{
				StartTs:    req.StartVersion,
				ConflictTs: ts,
				Key:        mu.Key,
				Primary:    req.PrimaryLock,
			}
			return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{{Conflict: writeConf}}}, nil

		}
		lock = &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindPut,
		}
		trac.PutLock(mu.Key, lock)
		if mu.Op == kvrpcpb.Op_Put {
			lock.Kind = mvcc.WriteKindPut
			trac.PutValue(mu.Key, mu.Value)
		} else if mu.Op == kvrpcpb.Op_Del {
			lock.Kind = mvcc.WriteKindDelete
			trac.DeleteValue(mu.Key)
		}

	}
	modify := trac.Writes()
	err = server.storage.Write(req.Context, modify)
	if nil != err {
		return &kvrpcpb.PrewriteResponse{}, err
	}
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if nil != err {
		return &kvrpcpb.CommitResponse{}, err
	}
	trac := mvcc.NewMvccTxn(reader, req.StartVersion)

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for i := 0; i < len(req.Keys); i++ {
		key := req.Keys[i]
		lock, err := trac.GetLock(key)
		if nil != err {
			return &kvrpcpb.CommitResponse{}, err
		}
		if nil == lock {
			return &kvrpcpb.CommitResponse{}, nil
		}
		if lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{Retryable: "true"}}, nil
		}
		write := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		}

		trac.PutWrite(key, req.CommitVersion, write)
		trac.DeleteLock(key)
	}
	modify := trac.Writes()
	err = server.storage.Write(req.Context, modify)
	if nil != err {
		return &kvrpcpb.CommitResponse{}, err
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if nil != err {
		return &kvrpcpb.ScanResponse{}, err
	}
	trac := mvcc.NewMvccTxn(reader, req.Version)
	iter := mvcc.NewScanner(req.StartKey, trac)
	defer iter.Close()

	var getPairs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); {
		key, value, err := iter.Next()
		if nil != err {
			return &kvrpcpb.ScanResponse{}, err
		}
		if nil == key {
			break
		}

		lock, err := trac.GetLock(key)
		if nil != err {
			return &kvrpcpb.ScanResponse{}, err
		}
		if nil != lock && lock.Ts <= req.Version {
			// key has been locked
			pair := &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					}},
				Key: key,
			}
			getPairs = append(getPairs, pair)
			i++
			continue
		}
		if nil != value {
			getPairs = append(getPairs, &kvrpcpb.KvPair{Key: key, Value: value})
			i++
		}
	}

	return &kvrpcpb.ScanResponse{Pairs: getPairs}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if nil != err {
		return &kvrpcpb.CheckTxnStatusResponse{}, err
	}

	trac := mvcc.NewMvccTxn(reader, req.LockTs)
	write, ts, err := trac.CurrentWrite(req.PrimaryKey)
	if nil != err {
		return &kvrpcpb.CheckTxnStatusResponse{}, err
	}
	if write != nil && write.StartTS == req.LockTs {
		//has roll back
		if write.Kind == mvcc.WriteKindRollback {
			return &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_NoAction}, nil
		} else {
			return &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_NoAction, CommitVersion: ts}, nil
		}

	}

	lock, err := trac.GetLock(req.PrimaryKey)
	if nil != err {
		return &kvrpcpb.CheckTxnStatusResponse{}, err
	}

	if lock == nil {
		rollback := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		trac.PutWrite(req.PrimaryKey, req.LockTs, rollback)
		modify := trac.Writes()
		err = server.storage.Write(req.Context, modify)
		if nil != err {
			return &kvrpcpb.CheckTxnStatusResponse{}, err
		}
		return &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_LockNotExistRollback}, nil
	}

	lockTime := mvcc.PhysicalTime(lock.Ts)
	currentTime := mvcc.PhysicalTime(req.CurrentTs)
	if currentTime-lockTime >= lock.Ttl {
		trac.DeleteLock(req.PrimaryKey)
		trac.DeleteValue(req.PrimaryKey)
		rollback := &mvcc.Write{
			StartTS: lock.Ts,
			Kind:    mvcc.WriteKindRollback,
		}
		trac.PutWrite(req.PrimaryKey, req.LockTs, rollback)
		modify := trac.Writes()
		err = server.storage.Write(req.Context, modify)
		if nil != err {
			return &kvrpcpb.CheckTxnStatusResponse{}, err
		}
		return &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_TTLExpireRollback}, nil
	}

	return &kvrpcpb.CheckTxnStatusResponse{}, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if nil != err {
		return &kvrpcpb.BatchRollbackResponse{}, err
	}
	trac := mvcc.NewMvccTxn(reader, req.StartVersion)

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	rollBack := &mvcc.Write{
		StartTS: req.StartVersion,
		Kind:    mvcc.WriteKindRollback,
	}
	for _, key := range req.Keys {
		//check wheater value is committed
		write, _, err := trac.CurrentWrite(key)
		if nil != err {
			return &kvrpcpb.BatchRollbackResponse{}, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				return &kvrpcpb.BatchRollbackResponse{}, nil
			} else {
				return &kvrpcpb.BatchRollbackResponse{Error: &kvrpcpb.KeyError{Abort: "true"}}, nil

			}
		}
		//check wheater is same tranaction or miss txn
		lock, err := trac.GetLock(key)
		if nil != err {
			return &kvrpcpb.BatchRollbackResponse{}, err
		}
		if lock == nil || lock.Ts == req.StartVersion {
			trac.DeleteLock(key)
			trac.DeleteValue(key)
		}

		trac.PutWrite(key, req.StartVersion, rollBack)
	}

	modify := trac.Writes()
	err = server.storage.Write(req.Context, modify)
	if nil != err {
		return &kvrpcpb.BatchRollbackResponse{}, err
	}

	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if nil != err {
		return &kvrpcpb.ResolveLockResponse{}, err
	}

	iterator := reader.IterCF(engine_util.CfLock)
	iterator.Close()
	var keys [][]byte
	for ; iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		value, err := item.Value()
		if nil != err {
			return &kvrpcpb.ResolveLockResponse{}, err
		}
		lock, err := mvcc.ParseLock(value)
		if nil != err {
			return &kvrpcpb.ResolveLockResponse{}, nil
		}
		if lock.Ts == req.StartVersion {
			key := item.Key()
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return &kvrpcpb.ResolveLockResponse{}, nil
	}
	if req.CommitVersion == 0 {
		resp1, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		return &kvrpcpb.ResolveLockResponse{Error: resp1.Error}, err
	} else {
		resp1, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		return &kvrpcpb.ResolveLockResponse{Error: resp1.Error}, err
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
