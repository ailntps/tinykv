package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

func main() {
	dir, err := ioutil.TempDir("", "engine_util")
	if nil != err {
		log.Fatal(err)
	}
	opts := badger.DefaultOptions
	//set the parameter of badger
	opts.Dir = dir
	opts.ValueDir = dir
	//open badger db
	db, err := badger.Open(opts)
	if nil != err {
		log.Fatal(err)
	}

	defer db.Close()

	// err = db.Update(func(txn *badger.Txn) error {
	// 	err := txn.Set([]byte("key1"), []byte("value1"))
	// 	err = txn.Set([]byte("key2"), []byte("value2"))
	// 	err = txn.Set([]byte("cf1_key1"), []byte("key1"))
	// 	err = txn.Set([]byte("cf1_key2"), []byte("key2"))
	// 	return err
	// })

	err = engine_util.PutCF(db, "cf1", []byte("key1"), []byte("value1"))
	err = engine_util.PutCF(db, "cf1", []byte("key2"), []byte("value2"))
	err = engine_util.PutCF(db, "cf1", []byte("key3"), []byte("value3"))

	str, _ := engine_util.GetCF(db, "cf1", []byte("key2"))
	fmt.Printf(string(str) + "\n")

	//read
	// err = db.View(func(txn *badger.Txn) error {
	// 	opts := badger.DefaultIteratorOptions
	// 	it := txn.NewIterator(opts)
	// 	defer it.Close()

	// 	for it.Rewind(); it.Valid(); it.Next() {
	// 		item := it.Item()
	// 		k := item.Key()
	// 		v, err := item.Value()
	// 		fmt.Printf("key =%s, value=%s\n", k, v)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// })
}
