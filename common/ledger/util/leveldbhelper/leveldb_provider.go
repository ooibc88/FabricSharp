/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leveldbhelper

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var dbNameKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)
var plogger = flogging.MustGetLogger("leveldbprovider")

type Prov struct {
	TxnID string
	Deps  []string
}

func lpad(s string, pad string, plength int) string {
	for i := len(s); i < plength; i++ {
		s = pad + s
	}
	return s
}

// Provider enables to use a single leveldb as multiple logical leveldbs
type Provider struct {
	db        *DB
	dbHandles map[string]*DBHandle
	mux       sync.Mutex
}

// NewProvider constructs a Provider
func NewProvider(conf *Conf) *Provider {
	db := CreateDB(conf)
	db.Open()
	return &Provider{db, make(map[string]*DBHandle), sync.Mutex{}}
}

// GetDBHandle returns a handle to a named db
func (p *Provider) GetDBHandle(dbName string) *DBHandle {
	p.mux.Lock()
	defer p.mux.Unlock()
	dbHandle := p.dbHandles[dbName]
	if dbHandle == nil {
		dbHandle = &DBHandle{dbName, p.db}
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
	dbName string
	db     *DB
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

func (h *DBHandle) HistQuery(key string, blkHeight uint64) (string, uint64, error) {
	histKey := constructHistKey(h.dbName, key, blkHeight)
	it := h.db.db.NewIterator(nil, h.db.readOpts)
	var err error
	committedBlkHeight := 0
	plogger.Infof("Historical query for Key: %s", histKey)
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
			if val, depCommittedHeight, _ := h.HistQuery(depKey, uint64(committedBlkHeight-1)); val == "" {
				return "", nil, nil, errors.New("Fail to find the entry for the dependent key " + depKey + " before blk " + strconv.Itoa(committedBlkHeight-1))
			} else {
				depBlkHeights = append(depBlkHeights, depCommittedHeight)
			}
		}
		return provResult.TxnID, provResult.Deps, depBlkHeights, nil

	}

	return "", nil, nil, nil // record not found
}

func (h *DBHandle) WriteProvBatch(batch *ProvUpdateBatch, sync bool) error {
	levelBatch := &leveldb.Batch{}
	levelBatch.Put(batch.SavePointKey, batch.SavePoint)
	for k, v := range batch.KVs {
		blkHeight := batch.BlkHeight[k]
		histKey := constructHistKey(h.dbName, k, blkHeight)
		// key := constructLevelKey(h.dbName, []byte(k))
		// plogger.Infof("Put key [%s]: [%s]", histKey, v)
		levelBatch.Put(histKey, v)

		var prov Prov
		prov.TxnID = batch.TxnIDs[k]
		// ledgerLogger.Infof("Long Key1: %s, val: %s, txnID: %s", long_key1, string(value), prov.TxnID)
		prov.Deps = make([]string, len(batch.KeyDeps[k]))
		copy(prov.Deps, batch.KeyDeps[k])
		provBytes, err := json.Marshal(prov)
		if err != nil {
			panic("Fail to marshal provenance")
		}
		provKey := constructProvKey(h.dbName, k, blkHeight)
		// plogger.Infof("Put key [%s]", provKey)
		levelBatch.Put(provKey, provBytes)

	}
	if err := h.db.WriteBatch(levelBatch, sync); err != nil {
		return err
	}
	return nil
}

// GetIterator gets an handle to iterator. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (h *DBHandle) GetIterator(startKey []byte, endKey []byte) *Iterator {
	sKey := constructLevelKey(h.dbName, startKey)
	eKey := constructLevelKey(h.dbName, endKey)
	if endKey == nil {
		// replace the last byte 'dbNameKeySep' by 'lastKeyIndicator'
		eKey[len(eKey)-1] = lastKeyIndicator
	}
	logger.Debugf("Getting iterator for range [%#v] - [%#v]", sKey, eKey)
	return &Iterator{h.db.GetIterator(sKey, eKey)}
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	KVs map[string][]byte
}

// NewUpdateBatch constructs an instance of a Batch
func NewUpdateBatch() *UpdateBatch {
	return &UpdateBatch{
		KVs: make(map[string][]byte),
	}
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

// ProvUpdateBatch encloses the details of multiple `updates`
type ProvUpdateBatch struct {
	SavePointKey []byte
	SavePoint    []byte
	KVs          map[string][]byte
	TxnIDs       map[string]string
	BlkHeight    map[string]uint64
	KeyDeps      map[string][]string
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
	}
}

// Put adds a KV
func (batch *ProvUpdateBatch) Put(key string, value []byte, txnID string, blkHeight uint64, deps []string) {
	batch.KVs[key] = value
	batch.TxnIDs[key] = txnID
	batch.BlkHeight[key] = blkHeight
	batch.KeyDeps[key] = deps
}

// Iterator extends actual leveldb iterator
type Iterator struct {
	iterator.Iterator
}

// Key wraps actual leveldb iterator method
func (itr *Iterator) Key() []byte {
	return retrieveAppKey(itr.Iterator.Key())
}

func constructLevelKey(dbName string, key []byte) []byte {
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}

func retrieveAppKey(levelKey []byte) []byte {
	return bytes.SplitN(levelKey, dbNameKeySep, 2)[1]
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
