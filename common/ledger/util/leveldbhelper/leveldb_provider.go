/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbhelper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/hyperledger/fabric/common/ledger/dataformat"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const (
	// internalDBName is used to keep track of data related to internals such as data format
	// _ is used as name because this is not allowed as a channelname
	internalDBName = "_"
	// maxBatchSize limits the memory usage (1MB) for a batch. It is measured by the total number of bytes
	// of all the keys in a batch.
	maxBatchSize = 1000000
)

var (
	dbNameKeySep     = []byte{0x00}
	lastKeyIndicator = byte(0x01)
	formatVersionKey = []byte{'f'} // a single key in db whose value indicates the version of the data format
)

type Prov struct {
	TxnID       string
	Deps        []string
	DepSnapshot uint64
}

type Forward struct {
	TxnIds     []string
	AntiDeps   []string
	BlkHeights []uint64
}

func lpad(s string, pad string, plength int) string {
	for i := len(s); i < plength; i++ {
		s = pad + s
	}
	return s
}

// due to the restriction of lpad
const MaxInt = 10000000

// closeFunc closes the db handle
type closeFunc func()

// Conf configuration for `Provider`
//
// `ExpectedFormat` is the expected value of the format key in the internal database.
// At the time of opening the db, A check is performed that
// either the db is empty (i.e., opening for the first time) or the value
// of the formatVersionKey is equal to `ExpectedFormat`. Otherwise, an error is returned.
// A nil value for ExpectedFormat indicates that the format is never set and hence there is no such record.
type Conf struct {
	DBPath         string
	ExpectedFormat string
}

// Provider enables to use a single leveldb as multiple logical leveldbs
type Provider struct {
	db *DB

	mux       sync.Mutex
	dbHandles map[string]*DBHandle
}

// NewProvider constructs a Provider
func NewProvider(conf *Conf) (*Provider, error) {
	db, err := openDBAndCheckFormat(conf)
	if err != nil {
		return nil, err
	}
	return &Provider{
		db:        db,
		dbHandles: make(map[string]*DBHandle),
	}, nil
}

func openDBAndCheckFormat(conf *Conf) (d *DB, e error) {
	db := CreateDB(conf)
	db.Open()

	defer func() {
		if e != nil {
			db.Close()
		}
	}()

	internalDB := &DBHandle{
		db:     db,
		dbName: internalDBName,
	}

	dbEmpty, err := db.IsEmpty()
	if err != nil {
		return nil, err
	}

	if dbEmpty && conf.ExpectedFormat != "" {
		logger.Infof("DB is empty Setting db format as %s", conf.ExpectedFormat)
		if err := internalDB.Put(formatVersionKey, []byte(conf.ExpectedFormat), true); err != nil {
			return nil, err
		}
		return db, nil
	}

	formatVersion, err := internalDB.Get(formatVersionKey)
	if err != nil {
		return nil, err
	}
	logger.Debugf("Checking for db format at path [%s]", conf.DBPath)

	if !bytes.Equal(formatVersion, []byte(conf.ExpectedFormat)) {
		logger.Errorf("The db at path [%s] contains data in unexpected format. expected data format = [%s] (%#v), data format = [%s] (%#v).",
			conf.DBPath, conf.ExpectedFormat, []byte(conf.ExpectedFormat), formatVersion, formatVersion)
		return nil, &dataformat.ErrFormatMismatch{
			ExpectedFormat: conf.ExpectedFormat,
			Format:         string(formatVersion),
			DBInfo:         fmt.Sprintf("leveldb at [%s]", conf.DBPath),
		}
	}
	logger.Debug("format is latest, nothing to do")
	return db, nil
}

// GetDataFormat returns the format of the data
func (p *Provider) GetDataFormat() (string, error) {
	f, err := p.GetDBHandle(internalDBName).Get(formatVersionKey)
	return string(f), err
}

// GetDBHandle returns a handle to a named db
func (p *Provider) GetDBHandle(dbName string) *DBHandle {
	p.mux.Lock()
	defer p.mux.Unlock()
	dbHandle := p.dbHandles[dbName]
	if dbHandle == nil {
		closeFunc := func() {
			p.mux.Lock()
			defer p.mux.Unlock()
			delete(p.dbHandles, dbName)
		}
		dbHandle = &DBHandle{dbName, p.db, closeFunc}
		p.dbHandles[dbName] = dbHandle
	}
	return dbHandle
}

// Close closes the underlying leveldb
func (p *Provider) Close() {
	p.db.Close()
}

// DBHandle is an handle to a named db
type DBHandle struct {
	dbName    string
	db        *DB
	closeFunc closeFunc
}

// Get returns the value for the given key
func (h *DBHandle) Get(key []byte) ([]byte, error) {
	return h.db.Get(constructLevelKey(h.dbName, key))
}

// Put saves the key/value
func (h *DBHandle) Put(key []byte, value []byte, sync bool) error {
	return h.db.Put(constructLevelKey(h.dbName, key), value, sync)
}

// Delete deletes the given key
func (h *DBHandle) Delete(key []byte, sync bool) error {
	return h.db.Delete(constructLevelKey(h.dbName, key), sync)
}

func (h *DBHandle) HistQuery(key string, blkHeight uint64) (string, uint64, error) {
	histKey := constructHistKey(h.dbName, key, blkHeight)
	it := h.db.db.NewIterator(nil, h.db.readOpts)
	var err error
	committedBlkHeight := 0
	// logger.Infof("Historical query for Key: %s", histKey)
	if it.Seek(histKey); it.Valid() {
		splits := strings.Split(string(it.Key()), "-")

		if len(splits) == 4 && splits[0] == h.dbName && splits[1] == "hist" && splits[2] == key {
			if committedBlkHeight, err = strconv.Atoi(splits[3]); err != nil {
				panic("Fail to parse blk index " + splits[3])
			} else if uint64(committedBlkHeight) == blkHeight {
				return string(it.Value()), blkHeight, nil
			}
		}
	}
	if it.Prev() {
		splits := strings.Split(string(it.Key()), "-")
		if len(splits) == 4 && splits[0] == h.dbName && splits[1] == "hist" && splits[2] == key {
			if committedBlkHeight, err = strconv.Atoi(splits[3]); err != nil {
				panic("Fail to parse blk index " + splits[3])
			}
			return string(it.Value()), uint64(committedBlkHeight), nil
		}
	}
	// return "", 0, errors.New("Not found") // record not found
	return "", 0, nil // record not found
}

func (h *DBHandle) Backward(key string, blkHeight uint64) (string, []string, []uint64, error) {
	provKey := constructProvKey(h.dbName, key, blkHeight)
	it := h.db.db.NewIterator(nil, h.db.readOpts)
	var err error
	committedBlkHeight := 0
	var provBytes []byte = nil

	if it.Seek(provKey); it.Valid() {
		splits := strings.Split(string(it.Key()), "-")

		if len(splits) == 4 && splits[0] == h.dbName && splits[1] == "prov" && splits[2] == key {
			if committedBlkHeight, err = strconv.Atoi(splits[3]); err != nil {
				panic("Fail to parse blk index from " + splits[3])
			} else if uint64(committedBlkHeight) == blkHeight {
				provBytes = it.Value()
			}
		}
	}
	if provBytes == nil && it.Prev() {
		splits := strings.Split(string(it.Key()), "-")
		if len(splits) == 4 && splits[0] == h.dbName && splits[1] == "prov" && splits[2] == key {
			if committedBlkHeight, err = strconv.Atoi(splits[3]); err != nil {
				panic("Fail to parse blk idx from " + splits[3])
			}
			provBytes = it.Value()
		}
	}

	if provBytes != nil {
		provResult := Prov{}
		if err := json.Unmarshal(it.Value(), &provResult); err != nil {
			return "", nil, nil, errors.New("Fail to unmarshal provenance record for key " + string(provKey))
		}

		// find the committed blk height for dependent key
		depBlkHeights := []uint64{}
		for _, depKey := range provResult.Deps {
			if val, depCommittedHeight, _ := h.HistQuery(depKey, uint64(provResult.DepSnapshot)); val == "" {
				return "", nil, nil, errors.New("Fail to find the entry for the dependent key " + depKey + " before blk " + strconv.Itoa(int(provResult.DepSnapshot)))
			} else {
				depBlkHeights = append(depBlkHeights, depCommittedHeight)
			}
		}
		return provResult.TxnID, provResult.Deps, depBlkHeights, nil

	}

	return "", nil, nil, errors.New("Not found") // record not found
}

func (h *DBHandle) Forward(key string, blkHeight uint64) ([]string, []string, []uint64, error) {
	forwardKey := constructForwardKey(h.dbName, key, blkHeight)
	it := h.db.db.NewIterator(nil, h.db.readOpts)
	var err error
	committedBlkHeight := 0
	var forwardBytes []byte = nil

	if it.Seek(forwardKey); it.Valid() {
		splits := strings.Split(string(it.Key()), "-")

		if len(splits) == 4 && splits[0] == h.dbName && splits[1] == "forward" && splits[2] == key {
			if committedBlkHeight, err = strconv.Atoi(splits[3]); err != nil {
				panic("Fail to parse blk index from " + splits[3])
			} else if uint64(committedBlkHeight) == blkHeight {
				forwardBytes = it.Value()
			}
		}
	}

	if forwardBytes == nil && it.Prev() {
		splits := strings.Split(string(it.Key()), "-")
		if len(splits) == 4 && splits[0] == h.dbName && splits[1] == "forward" && splits[2] == key {
			if committedBlkHeight, err = strconv.Atoi(splits[3]); err != nil {
				panic("Fail to parse blk idx from " + splits[3])
			}
			forwardBytes = it.Value()
		}
	}

	if forwardBytes != nil {
		forwardResult := Forward{}
		if err := json.Unmarshal(it.Value(), &forwardResult); err != nil {
			return nil, nil, nil, errors.New("Fail to unmarshal forward record for key " + string(forwardKey))
		}

		// find the committed blk height for dependent key
		return forwardResult.TxnIds, forwardResult.AntiDeps, forwardResult.BlkHeights, nil
	}
	return nil, nil, nil, errors.New("Not found") // record not found
}

func (h *DBHandle) WriteProvBatch(batch *ProvUpdateBatch, sync bool) error {
	levelBatch := &leveldb.Batch{}
	levelBatch.Put(batch.SavePointKey, batch.SavePoint)
	for k, v := range batch.KVs {
		blkHeight := batch.BlkHeight[k]
		histKey := constructHistKey(h.dbName, k, blkHeight)
		levelBatch.Put(histKey, v)

		var prov Prov
		prov.TxnID = batch.TxnIDs[k]
		// ledgerLogger.Infof("Long Key1: %s, val: %s, txnID: %s", long_key1, string(value), prov.TxnID)
		prov.Deps = make([]string, len(batch.KeyDeps[k]))
		prov.DepSnapshot = batch.DepSnapshots[k]
		copy(prov.Deps, batch.KeyDeps[k])
		provBytes, err := json.Marshal(prov)
		if err != nil {
			panic("Fail to marshal provenance")
		}
		provKey := constructProvKey(h.dbName, k, blkHeight)
		levelBatch.Put(provKey, provBytes)
		for _, depKey := range batch.KeyDeps[k] {
			if val, depCommittedHeight, _ := h.HistQuery(depKey, batch.DepSnapshots[k]); val == "" {
				return errors.New("Fail to find the entry for the dependent key " + depKey + " before (inclusive) blk " + strconv.Itoa(int(batch.DepSnapshots[k])))
			} else if forwardVal, err := h.db.Get([]byte(constructForwardKey(h.dbName, depKey, depCommittedHeight))); err != nil {
				return errors.New("Fail to find the forward entry for the key " + depKey + " at blk " + strconv.Itoa(int(depCommittedHeight)))
			} else {
				forwardEntry := Forward{}
				if err := json.Unmarshal(forwardVal, &forwardEntry); err != nil {
					return errors.New("Fail to unmarshal the forward entry for the key " + depKey + " at blk " + strconv.Itoa(int(depCommittedHeight)))
				}
				forwardEntry.AntiDeps = append(forwardEntry.AntiDeps, k)
				forwardEntry.TxnIds = append(forwardEntry.TxnIds, batch.TxnIDs[k])
				forwardEntry.BlkHeights = append(forwardEntry.BlkHeights, batch.BlkHeight[k])
				if newForwardBytes, err := json.Marshal(forwardEntry); err != nil {
					return errors.New("Fail to marshal forward entry for the key " + depKey + " at blk " + strconv.Itoa(int(depCommittedHeight)))
				} else {
					levelBatch.Put([]byte(constructForwardKey(h.dbName, depKey, depCommittedHeight)), newForwardBytes)
				}
			}
		}

		forwardKey := constructForwardKey(h.dbName, k, blkHeight)
		forwardBytes, _ := json.Marshal(Forward{}) // empty
		levelBatch.Put(forwardKey, forwardBytes)
	}
	if err := h.db.WriteBatch(levelBatch, sync); err != nil {
		return err
	}
	return nil
}

// DeleteAll deletes all the keys that belong to the channel (dbName).
func (h *DBHandle) DeleteAll() error {
	iter, err := h.GetIterator(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Release()

	// use leveldb iterator directly to be more efficient
	dbIter := iter.Iterator

	// This is common code shared by all the leveldb instances. Because each leveldb has its own key size pattern,
	// each batch is limited by memory usage instead of number of keys. Once the batch memory usage reaches maxBatchSize,
	// the batch will be committed.
	numKeys := 0
	batchSize := 0
	batch := &leveldb.Batch{}
	for dbIter.Next() {
		if err := dbIter.Error(); err != nil {
			return errors.Wrap(err, "internal leveldb error while retrieving data from db iterator")
		}
		key := dbIter.Key()
		numKeys++
		batchSize = batchSize + len(key)
		batch.Delete(key)
		if batchSize >= maxBatchSize {
			if err := h.db.WriteBatch(batch, true); err != nil {
				return err
			}
			logger.Infof("Have removed %d entries for channel %s in leveldb %s", numKeys, h.dbName, h.db.conf.DBPath)
			batchSize = 0
			batch = &leveldb.Batch{}
		}
	}
	if batch.Len() > 0 {
		return h.db.WriteBatch(batch, true)
	}
	return nil
}

// WriteBatch writes a batch in an atomic way
func (h *DBHandle) WriteBatch(batch *UpdateBatch, sync bool) error {
	if len(batch.KVs) == 0 {
		return nil
	}
	levelBatch := &leveldb.Batch{}
	for k, v := range batch.KVs {
		key := constructLevelKey(h.dbName, []byte(k))
		if v == nil {
			levelBatch.Delete(key)
		} else {
			levelBatch.Put(key, v)
		}
	}
	if err := h.db.WriteBatch(levelBatch, sync); err != nil {
		return err
	}
	return nil
}

// GetIterator gets an handle to iterator. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (h *DBHandle) GetIterator(startKey []byte, endKey []byte) (*Iterator, error) {
	sKey := constructLevelKey(h.dbName, startKey)
	eKey := constructLevelKey(h.dbName, endKey)
	if endKey == nil {
		// replace the last byte 'dbNameKeySep' by 'lastKeyIndicator'
		eKey[len(eKey)-1] = lastKeyIndicator
	}
	logger.Debugf("Getting iterator for range [%#v] - [%#v]", sKey, eKey)
	itr := h.db.GetIterator(sKey, eKey)
	if err := itr.Error(); err != nil {
		itr.Release()
		return nil, errors.Wrapf(err, "internal leveldb error while obtaining db iterator")
	}
	return &Iterator{h.dbName, itr}, nil
}

// Close closes the DBHandle after its db data have been deleted
func (h *DBHandle) Close() {
	if h.closeFunc != nil {
		h.closeFunc()
	}
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	KVs map[string][]byte
}

// NewUpdateBatch constructs an instance of a Batch
func NewUpdateBatch() *UpdateBatch {
	return &UpdateBatch{make(map[string][]byte)}
}

// Put adds a KV
func (batch *UpdateBatch) Put(key []byte, value []byte) {
	if value == nil {
		panic("Nil value not allowed")
	}
	batch.KVs[string(key)] = value
}

// Delete deletes a Key and associated value
func (batch *UpdateBatch) Delete(key []byte) {
	batch.KVs[string(key)] = nil
}

// Len returns the number of entries in the batch
func (batch *UpdateBatch) Len() int {
	return len(batch.KVs)
}

type ProvUpdateBatch struct {
	SavePointKey []byte
	SavePoint    []byte
	KVs          map[string][]byte
	TxnIDs       map[string]string
	BlkHeight    map[string]uint64
	KeyDeps      map[string][]string
	DepSnapshots map[string]uint64
}

// NewUpdateBatch constructs an instance of a Batch
func NewProvUpdateBatch(savePointKey, savePoint []byte) *ProvUpdateBatch {
	return &ProvUpdateBatch{
		SavePointKey: savePointKey,
		SavePoint:    savePoint,
		KVs:          make(map[string][]byte),
		TxnIDs:       make(map[string]string),
		BlkHeight:    make(map[string]uint64),
		KeyDeps:      make(map[string][]string),
		DepSnapshots: make(map[string]uint64),
	}
}

// Put adds a KV
func (batch *ProvUpdateBatch) Put(key string, value []byte, txnID string, blkHeight uint64, deps []string, depSnapshot uint64) {
	batch.KVs[key] = value
	batch.TxnIDs[key] = txnID
	batch.BlkHeight[key] = blkHeight
	batch.KeyDeps[key] = deps
	batch.DepSnapshots[key] = depSnapshot
}

// Iterator extends actual leveldb iterator
type Iterator struct {
	dbName string
	iterator.Iterator
}

// Key wraps actual leveldb iterator method
func (itr *Iterator) Key() []byte {
	return retrieveAppKey(itr.Iterator.Key())
}

// Seek moves the iterator to the first key/value pair
// whose key is greater than or equal to the given key.
// It returns whether such pair exist.
func (itr *Iterator) Seek(key []byte) bool {
	levelKey := constructLevelKey(itr.dbName, key)
	return itr.Iterator.Seek(levelKey)
}

func constructLevelKey(dbName string, key []byte) []byte {
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}

func constructHistKey(dbName, key string, blkHeight uint64) []byte {
	padBlkIdx := lpad(strconv.Itoa(int(blkHeight)), "0", 9)
	longKey := dbName + "-hist-" + key + "-" + padBlkIdx
	return []byte(longKey)
}

func constructProvKey(dbName, key string, blkHeight uint64) []byte {
	padBlkIdx := lpad(strconv.Itoa(int(blkHeight)), "0", 9)
	longKey := dbName + "-prov-" + key + "-" + padBlkIdx
	return []byte(longKey)
}

func constructForwardKey(dbName, key string, blkHeight uint64) []byte {
	padBlkIdx := lpad(strconv.Itoa(int(blkHeight)), "0", 9)
	longKey := dbName + "-forward-" + key + "-" + padBlkIdx
	return []byte(longKey)
}

func retrieveAppKey(levelKey []byte) []byte {
	return bytes.SplitN(levelKey, dbNameKeySep, 2)[1]
}
