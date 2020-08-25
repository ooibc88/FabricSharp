/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package stateustoredb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"
	"ustore"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/stateleveldb"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("stateustoredb")

// var compositeKeySep = []byte{0x00}
// var lastKeyIndicator = byte(0x01)
// var savePointKey = []byte{0x00}

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
}

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewVersionedDBProvider() (*VersionedDBProvider, error) {
	logger.Debug("constructing VersionedDBProvider for ustoredb")
	return &VersionedDBProvider{}, nil
}

// GetDBHandle gets the handle to a named database
func (provider *VersionedDBProvider) GetDBHandle(dbName string, namespaceProvider statedb.NamespaceProvider) (statedb.VersionedDB, error) {
	return newVersionedDB(ustore.NewKVDB(), dbName), nil
}

// Close closes the underlying db
func (provider *VersionedDBProvider) Close() {
}

// VersionedDB implements VersionedDB interface
type versionedDB struct {
	snapshotVersions map[uint64]string
	udb              ustore.KVDB
	dbName           string
}

// newVersionedDB constructs an instance of VersionedDB
func newVersionedDB(udb ustore.KVDB, dbName string) *versionedDB {
	return &versionedDB{make(map[uint64]string), udb, dbName}
}

// Open implements method in VersionedDB interface
func (vdb *versionedDB) Open() error {
	if status := vdb.udb.InitGlobalState(); !status.Ok() {
		return errors.New("Fail to init state with status" + status.ToString())
	} else {
		logger.Debug("INIT GLOBAL STATE SUCCEEDED")
	}
	return nil
}

func (vdb *versionedDB) BytesKeySupported() bool {
	return false
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
	return vdb.GetSnapshotState(math.MaxUint64, namespace, key)
}

// GetVersion implements method in VersionedDB interface
func (vdb *versionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	if localconfig.LineageSupported() && strings.HasSuffix(key, "_hist") {
		logger.Panicf("Shall not attempt to retrieve version for %s with lineage supported", key)
	} else if localconfig.LineageSupported() && strings.HasSuffix(key, "_backward") {
		logger.Panicf("Shall not attempt to retrieve version for %s with lineage supported", key)
	} else if localconfig.LineageSupported() && strings.HasSuffix(key, "_forward") {
		logger.Panicf("Shall not attempt to retrieve version for %s with lineage supported", key)
	} else if versionedValue, err := vdb.GetState(namespace, key); err != nil {
		return nil, err
	} else if versionedValue == nil {
		return nil, nil
	} else {
		return versionedValue.Version, nil
	}
	return nil, nil
}

func hash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return fmt.Sprintf("ns%d", h.Sum32())
}

func (vdb *versionedDB) GetSnapshotState(snapshot uint64, namespace string, key string) (*statedb.VersionedValue, error) {
	hashedNs := hash(namespace) // Hash the ns to avoid whitespace
	logger.Debugf("Snapshot Get original ns %s, hash_ns: %s, hashekey %s at snapshot %d", namespace, hashedNs, key, snapshot)
	zeroVer := version.NewHeight(0, 0)
	if strings.HasSuffix(key, "_hist") {
		splits := strings.Split(key, "_")
		originalKey := splits[0]
		queriedBlkIdx, err := strconv.Atoi(splits[1])
		if err != nil {
			return nil, errors.New("Fail to parse block index from Hist Query " + key)
		}

		var histResult statedb.HistResult
		compositeKey := encodeDataKey(hashedNs, originalKey)

		if histReturn := vdb.udb.Hist(compositeKey, uint64(queriedBlkIdx)); !histReturn.Status().Ok() {
			logger.Debugf("Fail to query historical state for Key %s, at blk_idx %d with status %s",
				compositeKey, queriedBlkIdx, histReturn.Status().ToString())
			histResult = statedb.HistResult{Msg: histReturn.Status().ToString(), Val: "", CreatedBlk: 0}
		} else {
			histVal := histReturn.Value()
			height := histReturn.Blk_idx()
			rawVal := ""
			if 0 < len(histVal) {
				if vv, err := stateleveldb.DecodeValue([]byte(histVal)); err != nil {
					logger.Panicf("Fail to decode value for %s ", histVal)
				} else {
					rawVal = string(vv.Value)
				}
			}
			logger.Debugf("ustoredb.Hist(%s, %d) = (%s, %d)", compositeKey, queriedBlkIdx, histVal, height)
			histResult = statedb.HistResult{Msg: "", Val: rawVal, CreatedBlk: height}
		}
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

		var backResult statedb.BackwardResult
		compositeKey := encodeDataKey(hashedNs, originalKey)
		if backReturn := vdb.udb.Backward(compositeKey, uint64(queriedBlkIdx)); !backReturn.Status().Ok() {
			logger.Debugf("Fail to backward query for Key %s at blk_idx %d with status %d", compositeKey, queriedBlkIdx, backReturn.Status().ToString())

			backResult = statedb.BackwardResult{Msg: backReturn.Status().ToString(), DepKeys: nil, DepBlkIdx: nil, TxnID: ""}
		} else {
			depKeys := make([]string, 0)
			depBlkIdxs := make([]uint64, 0)

			for i := 0; i < int(backReturn.Dep_keys().Size()); i++ {
				if ns, rawKey := decodeDataKey(backReturn.Dep_keys().Get(i)); ns != hashedNs {
					logger.Panicf("Inconsistent decoded ns, expected %s, actual %s", hashedNs, ns)
				} else {
					depKeys = append(depKeys, rawKey)

				}
				depBlkIdxs = append(depBlkIdxs, backReturn.Dep_blk_idx().Get(i))
			}

			logger.Debugf("ustoredb.Backward(%s, %d) = (%v, %v)", compositeKey, queriedBlkIdx, depKeys, depBlkIdxs)
			backResult = statedb.BackwardResult{Msg: "", DepKeys: depKeys, DepBlkIdx: depBlkIdxs, TxnID: backReturn.TxnID()}
		}
		if backJSON, err := json.Marshal(backResult); err != nil {
			return nil, errors.New("Fail to marshal for backResult")
		} else {
			return &statedb.VersionedValue{Version: zeroVer, Value: backJSON, Metadata: nil}, nil
		}
	} else if strings.HasSuffix(key, "_forward") {
		splits := strings.Split(key, "_")
		originalKey := splits[0]
		queriedBlkIdx, err := strconv.Atoi(splits[1])
		if err != nil {
			return nil, errors.New("Fail to parse block index from Forward Query " + key)
		}

		var forwardResult statedb.ForwardResult
		compositeKey := encodeDataKey(hashedNs, originalKey)
		if forwardReturn := vdb.udb.Forward(compositeKey, uint64(queriedBlkIdx)); !forwardReturn.Status().Ok() {
			logger.Debugf("Fail to forward query for Key %s at blk_idx %d with status %d", compositeKey, queriedBlkIdx, forwardReturn.Status().ToString())

			forwardResult = statedb.ForwardResult{Msg: forwardReturn.Status().ToString(), ForwardKeys: nil, ForwardBlkIdx: nil, ForwardTxnIDs: nil}
		} else {
			forKeys := make([]string, 0)
			forBlkIdxs := make([]uint64, 0)
			forTxnIDs := make([]string, 0)

			for i := 0; i < int(forwardReturn.Forward_keys().Size()); i++ {
				if ns, rawKey := decodeDataKey(forwardReturn.Forward_keys().Get(i)); ns != hashedNs {
					logger.Panicf("Inconsistent decoded ns, expected %s, actual %s", hashedNs, ns)
				} else {
					forKeys = append(forKeys, rawKey)
				}
				forBlkIdxs = append(forBlkIdxs, forwardReturn.Forward_blk_idx().Get(i))
				forTxnIDs = append(forTxnIDs, forwardReturn.TxnIDs().Get(i))
			}

			logger.Debugf("ustoredb.Forward(%s, %d) = (%v, %v, %v)", compositeKey, queriedBlkIdx, forKeys, forBlkIdxs, forTxnIDs)
			forwardResult = statedb.ForwardResult{Msg: "", ForwardKeys: forKeys, ForwardBlkIdx: forBlkIdxs, ForwardTxnIDs: forTxnIDs}
		}
		if forwardJSON, err := json.Marshal(forwardResult); err != nil {
			return nil, errors.New("Fail to marshal for forwardResult")
		} else {
			return &statedb.VersionedValue{Version: zeroVer, Value: forwardJSON, Metadata: nil}, nil
		}
	} else {
		compositeKey := encodeDataKey(hashedNs, key)
		if histResult := vdb.udb.Hist(compositeKey, snapshot); histResult.Status().IsNotFound() {
			return nil, nil
		} else if histResult.Status().Ok() {
			histVal := []byte(histResult.Value())
			if vv, err := stateleveldb.DecodeValue([]byte(histVal)); err != nil {
				return nil, err
			} else {
				logger.Debugf("ustoredb.SnapshotGetState(). hashedNs=%s (original %s), snapshot=%d, key=%s, val=%s, blk_idx=%d", hashedNs, namespace, snapshot, key, string(vv.Value), histResult.Blk_idx)
				return vv, nil
			}
		} else {
			return nil, errors.New("Fail to get state for Key " + compositeKey + " with status " + histResult.Status().ToString())
		}
	}
}

func (vdb *versionedDB) ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (statedb.QueryResultsIterator, error) {
	return nil, errors.New("ExecuteQueryWithMetadata not supported for ustoredb")
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *versionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	// dbBatch := leveldbhelper.NewUpdateBatch()
	namespaces := batch.GetUpdatedNamespaces()
	logger.Debugf("[udb] Prepare to commit blk %d", uint64(height.BlockNum))
	for i, ns := range namespaces {

		updates := batch.GetUpdates(ns)
		txnIds := batch.GetTxnIds(ns)
		deps := batch.GetDeps(ns)
		depSnapshots := batch.GetDepSnapshots(ns)

		oldNs := ns
		hashedNs := hash(ns) // Hash the ns to avoid whitespace
		logger.Debugf("[udb] Prepare to commit %d-th ns %s, hashNs %s", i, oldNs, hashedNs)

		for k, vv := range updates {
			dataKey := encodeDataKey(hashedNs, k)

			txnId := "default" // can not be empty
			if t, ok := txnIds[k]; ok {
				txnId = t
			}
			depList := ustore.NewVecStr()
			keyDeps := []string{}
			if d, ok := deps[k]; ok {
				for _, dk := range d {
					keyDeps = append(keyDeps, encodeDataKey(hashedNs, dk))
					depList.Add(encodeDataKey(hashedNs, dk))
				}
			}
			var depSnapshot uint64
			if height.BlockNum > 0 {
				depSnapshot = height.BlockNum - 1 // by default, simulate on the last block.
			}
			if s, ok := depSnapshots[k]; ok && s != math.MaxUint64 {
				depSnapshot = s
			}

			// startPut := time.Now()
			val := ""
			if vv.Value == nil {
				vdb.udb.PutState(dataKey, "", txnId, height.BlockNum, depList, vdb.snapshotVersions[depSnapshot])
				// dbBatch.Put(dataKey, []byte(""), txnId, height.BlockNum, keyDeps, depSnapshot)
			} else {
				if encodedVal, err := stateleveldb.EncodeValue(vv); err != nil {
					logger.Panicf("Fail to encode the value with msg %s", err.Error())
				} else {
					val = string(vv.Value)
					vdb.udb.PutState(dataKey, string(encodedVal), txnId, height.BlockNum, depList, vdb.snapshotVersions[depSnapshot])
				}
			}
			logger.Debugf("Channel [%s]: Applying key(string)=[%s] txnId=[%s] height=[%d] deps=[%v] depSnapshot=[%d] val=[%s]", vdb.dbName, dataKey, txnId, height.BlockNum, keyDeps, depSnapshot, val)
			// elapsedPut := time.Since(startPut).Nanoseconds() / 1000
		}
	}

	blkIdx := height.BlockNum
	startCommit := time.Now()
	logger.Debugf("[udb] Finish apply batch updates for block %d", blkIdx)
	if statusStr := vdb.udb.Commit(); !statusStr.GetFirst().Ok() {
		return errors.New("Fail to commit global state with status " + statusStr.GetFirst().ToString())
	} else {
		newVersion := statusStr.GetSecond()
		vdb.snapshotVersions[blkIdx] = newVersion
	}
	elapsedCommit := time.Since(startCommit).Nanoseconds() / 1000
	logger.Debugf("[udb] Finish commit state for block %d with %d us", blkIdx, elapsedCommit)
	if status := vdb.udb.Put("latest-height", strconv.Itoa(int(blkIdx))); !status.Ok() {
		return errors.New("Fail to put latest block height with status " + status.ToString())
	}
	return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *versionedDB) GetLatestSavePoint() (*version.Height, error) {
	if statusStr := vdb.udb.Get("latest-height"); !statusStr.GetFirst().Ok() {
		return nil, errors.New("Fail to get latest height with msg " + statusStr.GetFirst().ToString())
	} else if blkIdx, err := strconv.Atoi(statusStr.GetSecond()); err != nil {
		return nil, errors.New("Fail to parse latest blk idx from string")
	} else {
		return version.NewHeight(uint64(blkIdx), 0), nil
	}
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *versionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	return nil, errors.New("GetStateMultipleKeys not supported for ustoredb")
}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *versionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return nil, errors.New("ExecuteQuery not supported for ustoredb")
}

// ExecuteQueryWithMetadata implements method in VersionedDB interface
func (vdb *versionedDB) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	return nil, errors.New("ExecuteQueryWithMetadata not supported for ustoredb")
}

var dataKeyPrefix = []byte{'d'}
var nsKeySep = []byte{'#'}

func encodeDataKey(ns, key string) string {
	k := append(dataKeyPrefix, []byte(ns)...)
	k = append(k, nsKeySep...)
	k = append(k, []byte(key)...)
	return string(k)
}

func decodeDataKey(encodedDataKey string) (string, string) {
	split := bytes.SplitN([]byte(encodedDataKey), nsKeySep, 2)
	return string(split[0][1:]), string(split[1])
}

func (vdb *versionedDB) GetFullScanIterator(skipNamespace func(string) bool) (statedb.FullScanIterator, byte, error) {
	return nil, 0x00, errors.New("GetFullScanIterator not supported for ustoredb")

}

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (vdb *versionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	// return nil, errors.New("GetStateRangeScanIterator not supported for ustoredb")
	hashedNs := hash(namespace)
	startCKey := encodeDataKey(hashedNs, startKey)
	endCKey := encodeDataKey(hashedNs, endKey)
	logger.Debugf("Get State Range Scan for start key %s end key %s", startCKey, endCKey)
	dbItr := vdb.udb.Range(startCKey, endCKey)
	return newIterator(namespace, dbItr), nil

	// return vdb.GetStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, nil)
}

func (vdb *versionedDB) GetStateRangeScanIteratorWithPagination(namespace string, startKey string, endKey string, pageSize int32) (statedb.QueryResultsIterator, error) {
	return nil, errors.New("QueryResultsIterator not supported for ustoredb")
}

type Iterator struct {
	originalNs string
	dbItr      ustore.StateIterator
	atHead     bool
	closed     bool
}

func newIterator(originalNs string, dbItr ustore.StateIterator) *Iterator {
	return &Iterator{originalNs, dbItr, true, false}
}

func (it *Iterator) Next() (statedb.QueryResult, error) {
	if it.closed {
		logger.Panicf("Reference a closed iterator for ns %s", it.originalNs)
	}
	logger.Debugf("Next() on Iterator ns %s atHead %t", it.originalNs, it.atHead)
	if !it.dbItr.Valid() {
		logger.Debugf("Invalid Iterator")
		return nil, nil
	}
	var rawKey, rawVal string
	if it.atHead {
		rawKey = it.dbItr.Key()
		rawVal = it.dbItr.Value()
		it.atHead = false
	} else if hasNext := it.dbItr.Next(); !hasNext {
		return nil, nil
	} else {
		rawKey = it.dbItr.Key()
		rawVal = it.dbItr.Value()
	}

	if ns, key := decodeDataKey(rawKey); ns != hash(it.originalNs) {
		logger.Panicf("Expected hased ns: %s (original: %s), actual ns; %s", hash(it.originalNs), it.originalNs, ns)
		return nil, nil
	} else if vv, err := stateleveldb.DecodeValue([]byte(rawVal)); err != nil {
		return nil, err
	} else {
		logger.Debugf("Retrieve ns %s key %s val %s", it.originalNs, key, vv.Value)
		return &statedb.VersionedKV{
			CompositeKey:   statedb.CompositeKey{Namespace: it.originalNs, Key: key},
			VersionedValue: *vv}, nil
	}
}

func (it *Iterator) Close() {
	if it.closed {
		logger.Debugf("Already Close Iterator for ns %s before. Ignore this request. ", it.originalNs)
	} else {
		logger.Debugf("Successfully Close Iterator for ns %s", it.originalNs)
		ustore.DeleteStateIterator(it.dbItr)
		it.closed = true
	}
}

func (it *Iterator) GetBookmarkAndClose() string {
	if it.closed {
		logger.Panicf("Reference a closed iterator for ns %s", it.originalNs)
	}
	logger.Debugf("Close Iterator and BookMark for ns %s", it.originalNs)
	retval := ""
	if it.dbItr.Next() {
		if ns, key := decodeDataKey(it.dbItr.Key()); ns != hash(it.originalNs) {
			logger.Panicf("Expected hased ns: %s (original: %s), actual ns; %s", hash(it.originalNs), it.originalNs, ns)
		} else {
			retval = key
		}
	}
	it.Close()
	return retval
}
