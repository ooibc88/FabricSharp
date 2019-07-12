/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package snapshotbasedtxmgr

import (
	commonledger "github.com/hyperledger/fabric/common/ledger"
	ledger "github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
)

type queryHelper struct {
	txmgr        *SnapshotBasedTxMgr
	rwsetBuilder *rwsetutil.RWSetBuilder
	err          error
	doneInvoked  bool
	snapshot     uint64
}

func newQueryHelper(txmgr *SnapshotBasedTxMgr, rwsetBuilder *rwsetutil.RWSetBuilder) *queryHelper {
	helper := &queryHelper{txmgr: txmgr, rwsetBuilder: rwsetBuilder}
	helper.snapshot = txmgr.db.RetrieveLatestSnapshot()
	return helper
}

func (h *queryHelper) getState(ns string, key string) ([]byte, []byte, error) {
	versionedValue, err := h.txmgr.db.GetSnapshotState(h.snapshot, ns, key)
	if err != nil {
		return nil, nil, err
	}
	val, _, _ := decomposeVersionedValue(versionedValue)
	var ver version.Height
	ver.BlockNum = h.snapshot
	ver.TxNum = 0

	if h.rwsetBuilder != nil {
		h.rwsetBuilder.AddToReadSet(ns, key, &ver)
	}
	return val, nil, nil
}

func decomposeVersionedValue(versionedValue *statedb.VersionedValue) ([]byte, []byte, *version.Height) {
	var value []byte
	var metadata []byte
	var ver *version.Height
	if versionedValue != nil {
		value = versionedValue.Value
		ver = versionedValue.Version
		metadata = versionedValue.Metadata
	}
	return value, metadata, ver
}

func (h *queryHelper) getStateMultipleKeys(namespace string, keys []string) ([][]byte, error) {
	versionedValues, err := h.txmgr.db.GetStateMultipleKeys(namespace, keys)
	if err != nil {
		return nil, nil
	}
	values := make([][]byte, len(versionedValues))
	for i, versionedValue := range versionedValues {
		val, _, ver := decomposeVersionedValue(versionedValue)
		if h.rwsetBuilder != nil {
			h.rwsetBuilder.AddToReadSet(namespace, keys[i], ver)
		}
		values[i] = val
	}
	return values, nil
}

func (h *queryHelper) getStateRangeScanIterator(namespace string, startKey string, endKey string) (ledger.QueryResultsIterator, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) getStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) executeQuery(namespace, query string) (commonledger.ResultsIterator, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) executeQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) getPrivateData(ns, coll, key string) ([]byte, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) getPrivateDataValueHash(ns, coll, key string) (valueHash, metadataBytes []byte, err error) {
	panic("Not supported...")
	return nil, nil, nil
}

func (h *queryHelper) getPrivateDataMultipleKeys(ns, coll string, keys []string) ([][]byte, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) getPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (commonledger.ResultsIterator, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) executeQueryOnPrivateData(namespace, collection, query string) (commonledger.ResultsIterator, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) getStateMetadata(ns string, key string) (map[string][]byte, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) getPrivateDataMetadata(ns, coll, key string) (map[string][]byte, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) getPrivateDataMetadataByHash(ns, coll string, keyhash []byte) (map[string][]byte, error) {
	panic("Not supported...")
	return nil, nil
}

func (h *queryHelper) done() {
	if h.doneInvoked {
		return
	}

	defer func() {
		h.txmgr.db.ReleaseSnapshot(h.snapshot)
		h.doneInvoked = true
	}()
}
