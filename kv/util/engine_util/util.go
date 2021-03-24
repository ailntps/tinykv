package engine_util

// CF:the value of same key is not different in different CF
// it can be thought as a mini database,used to suport tranaction
import (
	"bytes"

	"github.com/Connor1996/badger"
	"github.com/golang/protobuf/proto"
)

//KeyWithCF :cf_key to ensure column family
func KeyWithCF(cf string, key []byte) []byte {
	return append([]byte(cf+"_"), key...)
}

//GetCF return CF value
func GetCF(db *badger.DB, cf string, key []byte) (val []byte, err error) {
	//*DB.View execute a read-only transaction
	err = db.View(func(txn *badger.Txn) error {
		val, err = GetCFFromTxn(txn, cf, key)
		return err
	})
	return
}

//GetCFFromTxn return the item of column family key "cf_key"
func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error) {
	//Get looks for key and returns corresponding Item. If key is not found, ErrKeyNotFound is returned.
	item, err := txn.Get(KeyWithCF(cf, key))
	if err != nil {
		return nil, err
	}
	//ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice
	val, err = item.ValueCopy(val)
	return
}

//PutCF put <cf,key>->value pair
func PutCF(engine *badger.DB, cf string, key []byte, val []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(KeyWithCF(cf, key), val)
	})
}

//GetMeta proto.Message is  a interface,read messaage from msg.bu why val don't store
func GetMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	var val []byte
	err := engine.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.Value()
		return err
	})
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

//GetMetaFromTxn get from transaction
func GetMetaFromTxn(txn *badger.Txn, key []byte, msg proto.Message) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	val, err := item.Value()
	if err != nil {
		return err
	}
	//msg decode,put metadata in msg
	return proto.Unmarshal(val, msg)
}

//PutMeta encode msg and store in db.
func PutMeta(engine *badger.DB, key []byte, msg proto.Message) error {
	//msg encode
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

//DeleteCF delete <cf,key>->value pair
func DeleteCF(engine *badger.DB, cf string, key []byte) error {
	return engine.Update(func(txn *badger.Txn) error {
		return txn.Delete(KeyWithCF(cf, key))
	})
}

func DeleteRange(db *badger.DB, startKey, endKey []byte) error {
	batch := new(WriteBatch)
	txn := db.NewTransaction(false)
	defer txn.Discard()
	for _, cf := range CFs {
		deleteRangeCF(txn, batch, cf, startKey, endKey)
	}

	return batch.WriteToDB(db)
}

func deleteRangeCF(txn *badger.Txn, batch *WriteBatch, cf string, startKey, endKey []byte) {
	it := NewCFIterator(cf, txn)
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if ExceedEndKey(key, endKey) {
			break
		}
		batch.DeleteCF(cf, key)
	}
	defer it.Close()
}

func ExceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}
