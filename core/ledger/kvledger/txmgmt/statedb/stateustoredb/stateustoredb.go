/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package stateustoredb

import (
	"encoding/json"
	"strconv"
	"strings"
	"ustore"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("stateustoredb")

var compositeKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)
var savePointKey = []byte{0x00}

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
}

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewVersionedDBProvider() *VersionedDBProvider {
	logger.Debug("constructing VersionedDBProvider for ustoredb")
	return &VersionedDBProvider{}
}

// GetDBHandle gets the handle to a named database
func (provider *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {
	return newVersionedDB(ustore.NewKVDB(), dbName), nil
}

// Close closes the underlying db
func (provider *VersionedDBProvider) Close() {
}

// VersionedDB implements VersionedDB interface
type versionedDB struct {
	udb    ustore.KVDB
	dbName string
}

// newVersionedDB constructs an instance of VersionedDB
func newVersionedDB(udb ustore.KVDB, dbName string) *versionedDB {
	return &versionedDB{udb, dbName}
}

// Open implements method in VersionedDB interface
func (vdb *versionedDB) Open() error {
	if status := vdb.udb.InitGlobalState(); !status.Ok() {
		return errors.New("Fail to init state with status" + status.ToString())
	}
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
		if histReturn := vdb.udb.Hist(compositeKey, uint64(queriedBlkIdx)); !histReturn.Status().Ok() {
			logger.Infof("Fail to query historical state for Key %s, at blk_idx %d with status %s",
				compositeKey, queriedBlkIdx, histReturn.Status().ToString())
			histResult = shim.HistResult{Msg: histReturn.Status().ToString(), Val: "", CreatedBlk: 0}
		} else {
			histVal := histReturn.Value()
			height := histReturn.Blk_idx()
			logger.Infof("ustoredb.Hist(%s, %d) = (%s, %d)", compositeKey, queriedBlkIdx, histVal, height)
			histResult = shim.HistResult{Msg: "", Val: histVal, CreatedBlk: height}
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

		var backResult shim.BackwardResult
		compositeKey := constructCompositeKey(namespace, originalKey)
		if backReturn := vdb.udb.Backward(compositeKey, uint64(queriedBlkIdx)); !backReturn.Status().Ok() {
			logger.Infof("Fail to backward query for Key %s at blk_idx %d with status %d", compositeKey, queriedBlkIdx, backReturn.Status().ToString())

			backResult = shim.BackwardResult{Msg: backReturn.Status().ToString(), DepKeys: nil, DepBlkIdx: nil, TxnID: ""}
		} else {
			depKeys := make([]string, 0)
			depBlkIdxs := make([]uint64, 0)

			for i := 0; i < int(backReturn.Dep_keys().Size()); i++ {
				depKeys = append(depKeys, backReturn.Dep_keys().Get(i))
				depBlkIdxs = append(depBlkIdxs, backReturn.Dep_blk_idx().Get(i))
			}

			logger.Infof("ustoredb.Backward(%s, %d) = (%v, %v)", compositeKey, queriedBlkIdx, depKeys, depBlkIdxs)
			backResult = shim.BackwardResult{Msg: "", DepKeys: depKeys, DepBlkIdx: depBlkIdxs, TxnID: backReturn.TxnID()}
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

		var forwardResult shim.ForwardResult
		compositeKey := constructCompositeKey(namespace, originalKey)
		if forwardReturn := vdb.udb.Forward(compositeKey, uint64(queriedBlkIdx)); !forwardReturn.Status().Ok() {
			logger.Infof("Fail to forward query for Key %s at blk_idx %d with status %d", compositeKey, queriedBlkIdx, forwardReturn.Status().ToString())

			forwardResult = shim.ForwardResult{Msg: forwardReturn.Status().ToString(), ForwardKeys: nil, ForwardBlkIdx: nil, ForwardTxnIDs: nil}
		} else {
			forKeys := make([]string, 0)
			forBlkIdxs := make([]uint64, 0)
			forTxnIDs := make([]string, 0)

			for i := 0; i < int(forwardReturn.Forward_keys().Size()); i++ {
				forKeys = append(forKeys, forwardReturn.Forward_keys().Get(i))
				forBlkIdxs = append(forBlkIdxs, forwardReturn.Forward_blk_idx().Get(i))
				forTxnIDs = append(forTxnIDs, forwardReturn.TxnIDs().Get(i))
			}

			logger.Infof("ustoredb.Backward(%s, %d) = (%v, %v, %v)", compositeKey, queriedBlkIdx, forKeys, forBlkIdxs, forTxnIDs)
			forwardResult = shim.ForwardResult{Msg: "", ForwardKeys: forKeys, ForwardBlkIdx: forBlkIdxs, ForwardTxnIDs: forTxnIDs}
		}
		if forwardJSON, err := json.Marshal(forwardResult); err != nil {
			return nil, errors.New("Fail to marshal for forwardResult")
		} else {
			return &statedb.VersionedValue{Version: zeroVer, Value: forwardJSON, Metadata: nil}, nil
		}
	} else {
		compositeKey := constructCompositeKey(namespace, key)
		if histResult := vdb.udb.Hist(compositeKey, 4294967295); histResult.Status().IsNotFound() {
			return nil, nil
		} else if histResult.Status().Ok() {
			val := []byte(histResult.Value())
			height := histResult.Blk_idx()
			logger.Infof("ustoredb.GetState(). ns=%s, key=%s, val=%s, blk_idx=%d", namespace, key, val, height)
			ver := version.NewHeight(height, 0)
			return &statedb.VersionedValue{Version: ver, Value: val, Metadata: nil}, nil
		} else {
			return nil, errors.New("Fail to get state for Key " + compositeKey + " with status " + histResult.Status().ToString())
		}
	}

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

// ApplyUpdates implements method in VersionedDB interface
func (vdb *versionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	// dbBatch := leveldbhelper.NewUpdateBatch()
	namespaces := batch.GetUpdatedNamespaces()
	for _, ns := range namespaces {
		updates := batch.GetUpdates(ns)
		for k, vv := range updates {
			compositeKey := constructCompositeKey(ns, k)
			logger.Infof("udb ApplyUpdates: Channel [%s]: Applying key(string)=[%s] value(string)=[%s]", vdb.dbName, string(compositeKey), string(vv.Value))
			if !strings.HasSuffix(k, "_prov") {
				logger.Infof("[udb] Key %s does NOT have prov suffix", k)
				val := string(vv.Value)
				depList := ustore.NewVecStr()
				depStrs := make([]string, 0)
				if provVal, ok := updates[k+"_prov"]; ok {
					depKeys := strings.Split(string(provVal.Value), "_")
					for _, depKey := range depKeys {
						if len(depKey) > 0 {
							depCompKey := constructCompositeKey(ns, depKey)
							depList.Add(depCompKey)
							depStrs = append(depStrs, depCompKey)
						}
					} // end for
				} // end if provVal
				txnID := "fakeTxnID" // Currently empty here
				logger.Infof("[udb] PutState key [%s], val [%s], blk idx [%d], dep_list [%v]", compositeKey, val, height.BlockNum, depStrs)
				vdb.udb.PutState(compositeKey, val, txnID, height.BlockNum, depList)
			} else {
				logger.Infof("[udb] Key %s has prov suffix", k)
			} // end if has Suffix
		}
	}
	blkIdx := height.BlockNum
	logger.Infof("[udb] Finish apply batch updates for block %d", blkIdx)
	if statusStr := vdb.udb.Commit(); !statusStr.GetFirst().Ok() {
		return errors.New("Fail to commit global state with status " + statusStr.GetFirst().ToString())
	}
	logger.Infof("[udb] Finish commit state for block %d", blkIdx)
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

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (vdb *versionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	return nil, errors.New("GetStateRangeScanIterator not supported for ustoredb")
	// return vdb.GetStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, nil)
}

// GetStateRangeScanIteratorWithMetadata implements method in VersionedDB interface
func (vdb *versionedDB) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	return nil, errors.New("GetStateRangeScanIteratorWithMetadata not supported for ustoredb")
}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *versionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return nil, errors.New("ExecuteQuery not supported for ustoredb")
}

// ExecuteQueryWithMetadata implements method in VersionedDB interface
func (vdb *versionedDB) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	return nil, errors.New("ExecuteQueryWithMetadata not supported for ustoredb")
}

func constructCompositeKey(ns string, key string) string {
	return ns + "#" + key
}
