package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/Connor1996/badger"
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
	bkey := func(i int) []byte {
		return []byte(fmt.Sprintf("%09d", i))
	}
	bval := func(i int) []byte {
		return []byte(fmt.Sprintf("%025d", i))
	}

	txn := db.NewTransaction(true)

	// Fill in 1000 items
	n := 1000
	for i := 0; i < n; i++ {
		err := txn.SetEntry(&badger.Entry{
			Key:   bkey(i),
			Value: bval(i)})
		if err != nil {
			panic(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		panic(err)
	}
	opt := badger.DefaultIteratorOptions

	// Iterate over 1000 items
	var count int
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Counted %d elements", count)

}
