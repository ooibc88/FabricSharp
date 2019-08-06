/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package stateleveldb

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var logger = flogging.MustGetLogger("stateleveldb")

var compositeKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)
var savePointKey = []byte{0x00}

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
	dbProvider *leveldbhelper.Provider
}

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewVersionedDBProvider() *VersionedDBProvider {
	dbPath := ledgerconfig.GetStateLevelDBPath()
	logger.Debugf("constructing VersionedDBProvider dbPath=%s", dbPath)
	dbProvider := leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})
	return &VersionedDBProvider{dbProvider}
}

// GetDBHandle gets the handle to a named database
func (provider *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {
	return newVersionedDB(provider.dbProvider.GetDBHandle(dbName), dbName), nil
}

// Close closes the underlying db
func (provider *VersionedDBProvider) Close() {
	provider.dbProvider.Close()
}

// VersionedDB implements VersionedDB interface
type versionedDB struct {
	db     *leveldbhelper.DBHandle
	dbName string
}

// newVersionedDB constructs an instance of VersionedDB
func newVersionedDB(db *leveldbhelper.DBHandle, dbName string) *versionedDB {
	return &versionedDB{db, dbName}
}

// Open implements method in VersionedDB interface
func (vdb *versionedDB) Open() error {
	// do nothing because shared db is used
	return nil
}

// Close implements method in VersionedDB interface
func (vdb *versionedDB) Close() {
	// do nothing because shared db is used
}

// ValidateKeyValue implements method in VersionedDB interface
func (vdb *versionedDB) ValidateKeyValue(key string, value []byte) error {
	return nil
}

// BytesKeySuppoted implements method in VersionedDB interface
func (vdb *versionedDB) BytesKeySuppoted() bool {
	return true
}

// GetState implements method in VersionedDB interface
func (vdb *versionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {

	zeroVer := version.NewHeight(0, 0)
	if strings.HasSuffix(key, "_hist") {
		splits := strings.Split(key, "_")
		originalKey := splits[0]
		queriedBlkIdx, err := strconv.Atoi(splits[1])
		if err != nil {
			return nil, errors.New("Fail to parse block index from Hist Query " + key)
		}

		var histResult shim.HistResult
		compositeKey := constructCompositeKey(namespace, originalKey)

		if val, blkIdx, err := vdb.db.HistQuery(compositeKey, uint64(queriedBlkIdx)); err != nil {
			histResult.Msg = err.Error()
		} else {
			histResult.CreatedBlk = blkIdx
			histResult.Val = val
			histResult.Msg = ""
		}

		logger.Infof("stateleveldb.Histquery(%s, %d) = (%s, %d, %d)", compositeKey, queriedBlkIdx, histResult.Val, histResult.CreatedBlk, histResult.Msg)
		if histJSON, err := json.Marshal(histResult); err != nil {
			return nil, errors.New("Fail to marshal for HistResult")
		} else {
			return &statedb.VersionedValue{Version: zeroVer, Value: histJSON, Metadata: nil}, nil
		}
	} else if strings.HasSuffix(key, "_backward") {
		splits := strings.Split(key, "_")
		originalKey := splits[0]
		queriedBlkIdx, err := strconv.Atoi(splits[1])
		if err != nil {
			return nil, errors.New("Fail to parse block index from Backward Query " + key)
		}

		var backResult shim.BackwardResult
		compositeKey := constructCompositeKey(namespace, originalKey)
		if txnID, depKeys, depBlkHeights, err := vdb.db.Backward(compositeKey, uint64(queriedBlkIdx)); err != nil {
			backResult.Msg = err.Error()
		} else {
			backResult.DepBlkIdx = depBlkHeights
			backResult.DepKeys = depKeys
			backResult.TxnID = txnID
			backResult.Msg = ""
		}
		logger.Infof("stateleveldb.Backward(%s, %d) = ([%v], [%v], %s, %s)", compositeKey, queriedBlkIdx, backResult.DepKeys, backResult.DepBlkIdx, backResult.TxnID, backResult.Msg)
		if backJSON, err := json.Marshal(backResult); err != nil {
			return nil, errors.New("Fail to marshal for backward query Result")
		} else {
			return &statedb.VersionedValue{Version: zeroVer, Value: backJSON, Metadata: nil}, nil
		}
	} else if strings.HasSuffix(key, "_forward") {
		var forResult shim.ForwardResult
		forResult.Msg = "Not Supported in current leveldb implementation..."
		if forwardJSON, err := json.Marshal(forResult); err != nil {
			return nil, errors.New("Fail to marshal for forward query result")
		} else {
			return &statedb.VersionedValue{Version: zeroVer, Value: forwardJSON, Metadata: nil}, nil
		}
	} else {
		// Normal query
		compositeKey := constructCompositeKey(namespace, key)
		// 4294967295 is an arbitrary large value so that val returns the up-to-date version.
		if val, _, err := vdb.db.HistQuery(compositeKey, 4294967295); err != nil {
			return nil, err
		} else if val == "" {
			return nil, nil
		} else {
			return decodeValue([]byte(val))
		}
	}

	// logger.Debugf("GetState(). ns=%s, key=%s", namespace, key)
	// compositeKey := constructCompositeKey(namespace, key)
	// dbVal, err := vdb.db.Get(compositeKey)
	// if err != nil {
	// 	return nil, err
	// }
	// if dbVal == nil {
	// 	return nil, nil
	// }
	// return decodeValue(dbVal)
	return nil, nil
}

// GetVersion implements method in VersionedDB interface
func (vdb *versionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	if strings.HasSuffix(key, "_hist") {
		return version.NewHeight(0, 0), nil
	} else if strings.HasSuffix(key, "_backward") {
		return version.NewHeight(0, 0), nil
	} else if strings.HasSuffix(key, "_forward") {
		return version.NewHeight(0, 0), nil
	} else {
		versionedValue, err := vdb.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		if versionedValue == nil {
			return nil, nil
		}
		return versionedValue.Version, nil
	}
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *versionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (vdb *versionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	return nil, errors.New("GetStateRangeScanIterator not supported for leveldb")
	// return vdb.GetStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, nil)
}

const optionLimit = "limit"

// GetStateRangeScanIteratorWithMetadata implements method in VersionedDB interface
func (vdb *versionedDB) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	return nil, errors.New("GetStateRangeScanIteratorWithMetadata not supported for leveldb")
	// requestedLimit := int32(0)
	// // if metadata is provided, validate and apply options
	// if metadata != nil {
	// 	//validate the metadata
	// 	err := statedb.ValidateRangeMetadata(metadata)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if limitOption, ok := metadata[optionLimit]; ok {
	// 		requestedLimit = limitOption.(int32)
	// 	}
	// }

	// // Note:  metadata is not used for the goleveldb implementation of the range query
	// compositeStartKey := constructCompositeKey(namespace, startKey)
	// compositeEndKey := constructCompositeKey(namespace, endKey)
	// if endKey == "" {
	// 	compositeEndKey[len(compositeEndKey)-1] = lastKeyIndicator
	// }
	// dbItr := vdb.db.GetIterator(compositeStartKey, compositeEndKey)

	// return newKVScanner(namespace, dbItr, requestedLimit), nil

}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *versionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return nil, errors.New("ExecuteQuery not supported for leveldb")
}

// ExecuteQueryWithMetadata implements method in VersionedDB interface
func (vdb *versionedDB) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	return nil, errors.New("ExecuteQueryWithMetadata not supported for leveldb")
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *versionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	dbBatch := leveldbhelper.NewProvUpdateBatch(savePointKey, height.ToBytes())
	namespaces := batch.GetUpdatedNamespaces()
	logger.Infof("STATELEVELDB.Prepare to commit blk %d", height.BlockNum)
	for _, ns := range namespaces {
		updates := batch.GetUpdates(ns)
		for k, vv := range updates {
			logger.Infof("STATELEVELDB.Put key [%s]", k)
			if !strings.HasSuffix(k, "_prov") && !strings.HasSuffix(k, "_txnID") {
				// this is a normal key update
				compositeKey := constructCompositeKey(ns, k)
				// if vv.Metadata == nil {
				// 	logger.Info("Empty meta...")
				// } else {
				// 	logger.Infof("Non-Empty meta... [%v]", vv.Metadata)
				// }
				// logger.Infof("[udb] Key %s does NOT have prov or txnID suffix", k)
				deps := make([]string, 0)
				if provVal, ok := updates[k+"_prov"]; ok {
					// find the provenance record for key k
					depKeys := strings.Split(string(provVal.Value), "_")
					for _, depKey := range depKeys {
						if len(depKey) > 0 {
							depCompKey := constructCompositeKey(ns, depKey)
							deps = append(deps, depCompKey)
						}
					} // end for
				} // end if provVal
				txnID := "faketxnid" // can NOT be empty string
				if txnIDVal, ok := updates[k+"_txnID"]; ok {
					txnID = string(txnIDVal.Value)
				}
				logger.Infof("ApplyUpdates: Channel [%s]: Applying key(string)=[%s] meta(string)=[%s] txnID=[%s], deps=[%v]", vdb.dbName, compositeKey, string(vv.Metadata), txnID, deps)

				var encodedVal []byte
				var err error
				if vv.Value != nil {
					encodedVal, err = encodeValue(vv)
					if err != nil {
						return err
					}
				}

				dbBatch.Put(compositeKey, encodedVal, txnID, height.BlockNum, deps)
			}

		}
	}
	// dbBatch.PutSavePointKey(savePointKey, height.ToBytes())
	// Setting sync to true as a precaution, false may be an ok optimization after further testing.
	if err := vdb.db.WriteProvBatch(dbBatch, true); err != nil {
		return err
	}
	return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *versionedDB) GetLatestSavePoint() (*version.Height, error) {
	versionBytes, err := vdb.db.Get(savePointKey)
	if err != nil {
		return nil, err
	}
	if versionBytes == nil {
		return nil, nil
	}
	version, _ := version.NewHeightFromBytes(versionBytes)
	return version, nil
}

func constructCompositeKey(ns string, key string) string {
	// return append(append([]byte(ns), compositeKeySep...), []byte(key)...)
	return ns + "*" + key
}

func splitCompositeKey(compositeKey []byte) (string, string) {
	splits := strings.SplitN(string(compositeKey), "*", 2)
	return splits[0], splits[1]
}

type kvScanner struct {
	namespace            string
	dbItr                iterator.Iterator
	requestedLimit       int32
	totalRecordsReturned int32
}

func newKVScanner(namespace string, dbItr iterator.Iterator, requestedLimit int32) *kvScanner {
	return &kvScanner{namespace, dbItr, requestedLimit, 0}
}

func (scanner *kvScanner) Next() (statedb.QueryResult, error) {

	if scanner.requestedLimit > 0 && scanner.totalRecordsReturned >= scanner.requestedLimit {
		return nil, nil
	}

	if !scanner.dbItr.Next() {
		return nil, nil
	}

	dbKey := scanner.dbItr.Key()
	dbVal := scanner.dbItr.Value()
	dbValCopy := make([]byte, len(dbVal))
	copy(dbValCopy, dbVal)
	_, key := splitCompositeKey(dbKey)
	vv, err := decodeValue(dbValCopy)
	if err != nil {
		return nil, err
	}

	scanner.totalRecordsReturned++

	return &statedb.VersionedKV{
		CompositeKey: statedb.CompositeKey{Namespace: scanner.namespace, Key: key},
		// TODO remove dereferrencing below by changing the type of the field
		// `VersionedValue` in `statedb.VersionedKV` to a pointer
		VersionedValue: *vv}, nil
}

func (scanner *kvScanner) Close() {
	scanner.dbItr.Release()
}

func (scanner *kvScanner) GetBookmarkAndClose() string {
	retval := ""
	if scanner.dbItr.Next() {
		dbKey := scanner.dbItr.Key()
		_, key := splitCompositeKey(dbKey)
		retval = key
	}
	scanner.Close()
	return retval
}
