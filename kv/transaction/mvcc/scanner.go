package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.

type Scanner struct {
	// Your Data Here (4C).
	nextKey []byte
	finish  bool
	txn     *MvccTxn
	iter    engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	scan := &Scanner{
		txn:     txn,
		finish:  false,
		iter:    iter,
		nextKey: startKey,
	}

	return scan
}

func (scan *Scanner) Close() {
	scan.iter.Close()
	// Your Code Here (4C).
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	//format like tranaction's getValue(),check wheather committed
	//the different is need manager nextKey and finished argument
	if scan.finish == true {
		return nil, nil, nil
	}
	reqKey := scan.nextKey
	//ts is decending,find the most near commite operator
	scan.iter.Seek(EncodeKey(reqKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		scan.finish = true
		return nil, nil, nil
	}
	item := scan.iter.Item()
	writeKey := item.Key()
	if !bytes.Equal(DecodeUserKey(writeKey), reqKey) {
		//no reqkey write
		scan.nextKey = DecodeUserKey(writeKey)
		return scan.Next()
	}
	//find nextKey
	for {
		scan.iter.Next()
		if !scan.iter.Valid() {
			scan.finish = true
			break
		}
		getItem := scan.iter.Item()
		getKey := DecodeUserKey(getItem.Key())
		if !bytes.Equal(getKey, reqKey) {
			scan.nextKey = getKey
			break
		}
	}
	value, err := item.Value()
	if nil != err {
		return reqKey, nil, err
	}
	write, err := ParseWrite(value)
	if nil != err {
		return reqKey, nil, err
	}
	if write.Kind == WriteKindDelete {
		return reqKey, nil, nil
	}
	v, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(reqKey, write.StartTS))
	return reqKey, v, err
}
