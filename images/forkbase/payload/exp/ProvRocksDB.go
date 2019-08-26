package microexp

import (
	"encoding/json"
	"fmt"
	"microexp/tecbot/gorocksdb"
	"strconv"
	"strings"
)

func lpad(s string, pad string, plength int) string {
	for i := len(s); i < plength; i++ {
		s = pad + s
	}
	return s
}

type Prov struct {
	TxnID string
	Deps  []string
}

type ProvRocksDB struct {
	db *gorocksdb.DB
	wb *gorocksdb.WriteBatch
	wo *gorocksdb.WriteOptions
	ro *gorocksdb.ReadOptions
}

func NewProvRocksDB(dbPath string) ProvDB {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	// var err error
	// var rawdb *gorocksdb.DB
	if rawdb, err := gorocksdb.OpenDb(opts, dbPath); err != nil {
		panic("Fail to create rocksdb instance with err " + err.Error())
	} else {
		fmt.Println("Create rocksdb successfully...")
		return &ProvRocksDB{db: rawdb,
			wb: gorocksdb.NewWriteBatch(),
			wo: gorocksdb.NewDefaultWriteOptions(),
			ro: gorocksdb.NewDefaultReadOptions()}
	}
}

func (rdb *ProvRocksDB) Put(key, value, txnID string, blk uint64, depKeys []string) error {
	if rdb.db == nil {
		panic("Empty rocksdb instance...")
	}
	padBlkIdx := lpad(strconv.Itoa(int(blk)), "0", 9)
	// ledgerLogger.Info("Pad Blk Idx: ", pad_blk_idx)
	longKey1 := "hist-" + key + "-" + padBlkIdx
	longKey2 := "prov-" + key + "-" + padBlkIdx
	rdb.wb.Put([]byte(longKey1), []byte(value))

	var prov Prov
	prov.TxnID = txnID
	// ledgerLogger.Infof("Long Key1: %s, val: %s, txnID: %s", long_key1, string(value), prov.TxnID)
	prov.Deps = make([]string, len(depKeys))
	copy(prov.Deps, depKeys)
	b, err := json.Marshal(prov)
	if err != nil {
		panic("Fail to marshal provenance")
	}
	// ledgerLogger.Infof("Long Key2: %s", long_key2)
	rdb.wb.Put([]byte(longKey2), b)

	return nil
}

func (rdb *ProvRocksDB) Hist(key string, blk uint64) (string, uint64, error) {
	var err error
	var blkIdx int
	it := rdb.db.NewIterator(rdb.ro)
	padBlkIdx := lpad(strconv.Itoa(int(blk)), "0", 9)
	longKey := []byte("hist-" + key + "-" + padBlkIdx)
	if it.Seek(longKey); it.Valid() {
		splits := strings.Split(string(it.Key().Data()), "-")
		identical := len(splits) == 3 && splits[0] == "hist" && splits[1] == key
		if identical {
			if blkIdx, err = strconv.Atoi(splits[2]); err != nil {
				panic("Fail to parse blk idx from " + splits[2])
			} else if uint64(blkIdx) == blk {
				// fmt.Println(string(it.Value().Data()))
				return string(it.Value().Data()), uint64(blkIdx), nil
			}

		}
	}

	if it.Prev(); it.Valid() {
		splits := strings.Split(string(it.Key().Data()), "-")
		qualified := len(splits) == 3 && splits[0] == "hist" && splits[1] == key
		if qualified {
			if blkIdx, err = strconv.Atoi(splits[2]); err != nil {
				panic("Fail to parse blk idx from " + splits[2])
			}
			// fmt.Println(string(it.Value().Data()))
			return string(it.Value().Data()), uint64(blkIdx), nil
		}
	}
	return "", 0, nil // not found, nil for error
}

func (rdb *ProvRocksDB) Backward(key string, blk uint64) (string, []string, []uint64, error) {
	it := rdb.db.NewIterator(rdb.ro)
	longKey := []byte("prov-" + key + "-" + lpad(strconv.Itoa(int(blk)), "0", 9))
	var err error
	var provValue []byte
	var committedBlkIdx int
	if it.Seek(longKey); it.Valid() {
		splits := strings.Split(string(it.Key().Data()), "-")
		identical := len(splits) == 3 && splits[0] == "prov" && splits[1] == key
		if identical {
			if committedBlkIdx, err = strconv.Atoi(splits[2]); err != nil {
				panic("Fail to parse blk idx from " + splits[2])
			} else if uint64(committedBlkIdx) == blk {
				provValue = it.Value().Data()
			}
		}
	}

	if provValue == nil {
		if it.Prev(); it.Valid() {
			splits := strings.Split(string(it.Key().Data()), "-")
			if len(splits) == 3 && splits[0] == "prov" && splits[1] == key {
				if committedBlkIdx, err = strconv.Atoi(splits[2]); err != nil {
					panic("Fail to parse blk idx from " + splits[2])
				}
				provValue = it.Value().Data()
			}
		}
	}
	if provValue != nil {
		var prov Prov
		err := json.Unmarshal(provValue, &prov)
		if err != nil {
			panic("Fail to unmarshal the provenance record")
		}
		blkIdxs := make([]uint64, 0)
		for _, depKey := range prov.Deps {
			if _, depCommitedBlk, err := rdb.Hist(depKey, uint64(committedBlkIdx-1)); err != nil {
				panic("Fail to find entry for dependent key " + depKey)
			} else {
				blkIdxs = append(blkIdxs, depCommitedBlk)
			}
		}
		return prov.TxnID, prov.Deps, blkIdxs, nil

	}

	return "", nil, nil, nil
}
func (rdb *ProvRocksDB) Forward(key string, blk uint64) ([]string, []string, []uint64, error) {
	panic("Not Supported")
	return nil, nil, nil, nil
}

func (rdb *ProvRocksDB) Commit() (string, error) {
	if err := rdb.db.Write(rdb.wo, rdb.wb); err != nil {
		fmt.Println("Error: ", err.Error())
		panic("Fail to commit")
	}
	rdb.wb.Clear()
	return "N.A.", nil
}

func (rdb *ProvRocksDB) Scan(key string) {
	longKey := "hist-" + "-" + key
	it := rdb.db.NewIterator(rdb.ro)
	for it.Seek([]byte(longKey)); it.Valid(); it.Next() {
		splits := strings.Split(string(it.Key().Data()), "-")
		// ledgerLogger.Infof("Key: %s", string(it.Key().Data()))
		// ledgerLogger.Infof("Splits: %v", splits)
		if len(splits) != 3 || splits[0] != "hist" || key < splits[1] {
			break
		}
		it.Value().Data()
	}
	return
}
