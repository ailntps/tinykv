package mvcc

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {

	wr := storage.Put{
		Key:   EncodeKey(key, ts),
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}
	modify := storage.Modify{
		Data: wr,
	}
	txn.writes = append(txn.writes, modify)
	// Your Code Here (4A).
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	value, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if nil != err {
		return nil, err
	}
	if nil == value {
		return nil, nil
	}
	lock, err := ParseLock(value)
	if nil != err {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	lo := storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}

	modify := storage.Modify{
		Data: lo,
	}
	txn.writes = append(txn.writes, modify)
	// Your Code Here (4A).
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	delete := storage.Delete{
		Cf:  engine_util.CfLock,
		Key: key,
	}
	modify := storage.Modify{
		Data: delete,
	}
	txn.writes = append(txn.writes, modify)
	// Your Code Here (4A).
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	defer iterator.Close()

	iterator.Seek(EncodeKey(key, txn.StartTS))
	if !iterator.Valid() {
		return nil, nil
	}

	item := iterator.Item()
	value, err := item.Value()
	if nil != err {
		return nil, err
	}
	if !bytes.Equal(key, DecodeUserKey(item.Key())) {
		return nil, nil
	}
	write, err := ParseWrite(value)
	if nil != err {
		return nil, err
	}
	if write.Kind == WriteKindDelete {
		return nil, nil
	}

	v, err := txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	if err != nil {
		return nil, err
	}
	return v, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	va := storage.Put{
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	modify := storage.Modify{
		Data: va,
	}
	txn.writes = append(txn.writes, modify)
	// Your Code Here (4A).
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	delete := storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf:  engine_util.CfDefault,
	}
	modify := storage.Modify{
		Data: delete,
	}
	txn.writes = append(txn.writes, modify)
	// Your Code Here (4A).
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	defer iterator.Close()
	for ; iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		value, err := item.Value()
		if nil != err {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if nil != err {
			return nil, 0, err
		}
		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(item.Key()), nil
		}
	}

	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	defer iterator.Close()
	for ; iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		k := item.Key()

		uk := DecodeUserKey(k)
		if bytes.Equal(uk, key) {
			Value, err := item.Value()
			if nil != err {
				return nil, 0, err
			}
			write, err := ParseWrite(Value)
			if nil != err {
				return nil, 0, err
			}
			return write, decodeTimestamp(k), nil
		}
	}
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	//put ts is bigEndian,ts is uint64,8 bytes.
	//ts1 =[0,0,0,0,0,0,0,1] ts2=[0,0,0,0,0,0,0,2]
	//newkey1=[...,0xFE,0xFF,.....] newkey2=[....,0xFD,0xFF,....]
	//this is descending order
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
