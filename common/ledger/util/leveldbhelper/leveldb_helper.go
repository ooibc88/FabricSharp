/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbhelper

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	goleveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var logger = flogging.MustGetLogger("leveldbhelper")

type dbState int32

const (
	closed dbState = iota
	opened
)

// Conf configuration for `DB`
type Conf struct {
	DBPath string
}

type CountSnapshot struct {
	count    uint64
	snapshot *leveldb.Snapshot
}

// DB - a wrapper on an actual store
type DB struct {
	conf    *Conf
	db      *leveldb.DB
	dbState dbState
	mux     sync.Mutex

	countSnapshotMap sync.Map
	lastSnapshot     uint64

	readOpts        *opt.ReadOptions
	writeOptsNoSync *opt.WriteOptions
	writeOptsSync   *opt.WriteOptions
}

// CreateDB constructs a `DB`
func CreateDB(conf *Conf) *DB {
	readOpts := &opt.ReadOptions{}
	writeOptsNoSync := &opt.WriteOptions{}
	writeOptsSync := &opt.WriteOptions{}
	writeOptsSync.Sync = true

	return &DB{
		conf:            conf,
		dbState:         closed,
		readOpts:        readOpts,
		lastSnapshot:    0,
		writeOptsNoSync: writeOptsNoSync,
		writeOptsSync:   writeOptsSync}
}

// Open opens the underlying db
func (dbInst *DB) Open() {
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbState == opened {
		return
	}
	dbOpts := &opt.Options{}
	dbPath := dbInst.conf.DBPath
	var err error
	var dirEmpty bool
	if dirEmpty, err = util.CreateDirIfMissing(dbPath); err != nil {
		panic(fmt.Sprintf("Error creating dir if missing: %s", err))
	}
	dbOpts.ErrorIfMissing = !dirEmpty
	if dbInst.db, err = leveldb.OpenFile(dbPath, dbOpts); err != nil {
		panic(fmt.Sprintf("Error opening leveldb: %s", err))
	}
	if firstSnapshot, err := dbInst.db.GetSnapshot(); err != nil {
		panic(fmt.Sprintf("Error creating snapshot: %s", err))
	} else {
		dbInst.countSnapshotMap.Store(dbInst.lastSnapshot, &CountSnapshot{count: 0, snapshot: firstSnapshot})
	}
	dbInst.dbState = opened

	go func() {
		for {
			toRemoved := []uint64{}
			count := 0
			dbInst.countSnapshotMap.Range(func(key, value interface{}) bool {
				if value.(*CountSnapshot).count == 0 && key.(uint64) < atomic.LoadUint64(&dbInst.lastSnapshot) {
					toRemoved = append(toRemoved, key.(uint64))
				}
				count++
				return true
			})

			for _, r := range toRemoved {
				dbInst.countSnapshotMap.Delete(r)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
}

// Close closes the underlying db
func (dbInst *DB) Close() {
	dbInst.mux.Lock()
	defer dbInst.mux.Unlock()
	if dbInst.dbState == closed {
		return
	}
	if err := dbInst.db.Close(); err != nil {
		logger.Errorf("Error closing leveldb: %s", err)
	}
	dbInst.dbState = closed
}

// Get returns the value for the given key
func (dbInst *DB) Get(key []byte) ([]byte, error) {
	value, err := dbInst.db.Get(key, dbInst.readOpts)
	if err == leveldb.ErrNotFound {
		value = nil
		err = nil
	}
	if err != nil {
		logger.Errorf("Error retrieving leveldb key [%#v]: %s", key, err)
		return nil, errors.Wrapf(err, "error retrieving leveldb key [%#v]", key)
	}
	return value, nil
}

func (dbInst *DB) RetrieveLatestSnapshot() uint64 {
	lastest := atomic.LoadUint64(&dbInst.lastSnapshot)
	if countSnapshotVal, ok := dbInst.countSnapshotMap.Load(lastest); !ok {
		panic("Fail to load countSnapshotMap for snapshot " + strconv.Itoa(int(lastest)))
	} else {
		countSnapshotVal.(*CountSnapshot).count++
		logger.Infof("Increment latest snapshot %d to count %d.", lastest, countSnapshotVal.(*CountSnapshot))
	}
	return lastest
}

func (dbInst *DB) ReleaseSnapshot(snapshot uint64) bool {
	lastest := atomic.LoadUint64(&dbInst.lastSnapshot)
	if countSnapshotVal, ok := dbInst.countSnapshotMap.Load(snapshot); !ok {
		panic("Fail to load countSnapshotMap for snapshot " + strconv.Itoa(int(snapshot)))
	} else {
		countSnapshotVal.(*CountSnapshot).count--
		logger.Infof("Decrement snapshot %d to count %d. Latest Snapshot: %d", snapshot, countSnapshotVal, lastest)
	}
	return false
}

func (dbInst *DB) SnapshotGet(snapshot uint64, key []byte) ([]byte, error) {
	if countSnapshotVal, ok := dbInst.countSnapshotMap.Load(snapshot); !ok {
		panic("Fail to load countSnapshotMap for snapshot " + strconv.Itoa(int(snapshot)))
	} else {
		value, err := countSnapshotVal.(*CountSnapshot).snapshot.Get(key, dbInst.readOpts)
		if err == leveldb.ErrNotFound {
			value = nil
			err = nil
		}
		if err != nil {
			logger.Errorf("Error retrieving leveldb key [%#v] from snapshot %d : %s", key, snapshot, err)
			return nil, errors.Wrapf(err, "error retrieving leveldb key [%#v]", key)
		}
		return value, nil
	}
	return nil, nil
}

// Put saves the key/value
func (dbInst *DB) Put(key []byte, value []byte, sync bool) error {
	wo := dbInst.writeOptsNoSync
	if sync {
		wo = dbInst.writeOptsSync
	}
	err := dbInst.db.Put(key, value, wo)
	if err != nil {
		logger.Errorf("Error writing leveldb key [%#v]", key)
		return errors.Wrapf(err, "error writing leveldb key [%#v]", key)
	}
	return nil
}

// Delete deletes the given key
func (dbInst *DB) Delete(key []byte, sync bool) error {
	wo := dbInst.writeOptsNoSync
	if sync {
		wo = dbInst.writeOptsSync
	}
	err := dbInst.db.Delete(key, wo)
	if err != nil {
		logger.Errorf("Error deleting leveldb key [%#v]", key)
		return errors.Wrapf(err, "error deleting leveldb key [%#v]", key)
	}
	return nil
}

// GetIterator returns an iterator over key-value store. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (dbInst *DB) GetIterator(startKey []byte, endKey []byte) iterator.Iterator {

	// return dbInst.snapshot.NewIterator(&goleveldbutil.Range{Start: startKey, Limit: endKey}, dbInst.readOpts)
	return dbInst.db.NewIterator(&goleveldbutil.Range{Start: startKey, Limit: endKey}, dbInst.readOpts)
}

func (dbInst *DB) SnapshotGetIterator(snapshot uint64, startKey []byte, endKey []byte) iterator.Iterator {
	if countSnapshotVal, ok := dbInst.countSnapshotMap.Load(snapshot); !ok {
		panic("Fail to load countSnapshotMap for snapshot " + strconv.Itoa(int(snapshot)))
	} else {
		return countSnapshotVal.(*CountSnapshot).snapshot.NewIterator(&goleveldbutil.Range{Start: startKey, Limit: endKey}, dbInst.readOpts)
	}
}

// WriteBatch writes a batch
func (dbInst *DB) WriteBatch(batch *leveldb.Batch, sync bool) error {
	wo := dbInst.writeOptsNoSync
	if sync {
		wo = dbInst.writeOptsSync
	}
	if err := dbInst.db.Write(batch, wo); err != nil {
		return errors.Wrap(err, "error writing batch to leveldb")
	}
	return nil
}

func (dbInst *DB) SnapshotWriteBatch(batch *leveldb.Batch, blkHeight uint64, synchronized bool) error {
	// dbInst.snapshot.Release()
	var err error
	if err = dbInst.WriteBatch(batch, synchronized); err != nil {
		return err
	}
	if newSnapshot, err := dbInst.db.GetSnapshot(); err != nil {
		panic(fmt.Sprintf("Error creating snapshot %s", err))
	} else {
		dbInst.countSnapshotMap.Store(blkHeight, &CountSnapshot{count: 0, snapshot: newSnapshot})
		atomic.StoreUint64(&dbInst.lastSnapshot, blkHeight)
		logger.Infof("Create the latest snapshot %d", blkHeight)
	}
	return err
}
