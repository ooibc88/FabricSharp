/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package snapshotbasedtxmgr

import (
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger"
)

// SnapshotBasedQueryExecutor is a query executor used in `SnapshotBasedTxMgr`
type SnapshotBasedQueryExecutor struct {
	helper *queryHelper
	txid   string
}

func newSnapshotQueryExecutor(txmgr *SnapshotBasedTxMgr, txid string) *SnapshotBasedQueryExecutor {
	helper := newQueryHelper(txmgr, nil)
	logger.Debugf("constructing new query executor txid = [%s]", txid)
	return &SnapshotBasedQueryExecutor{helper, txid}
}

// GetState implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) GetState(ns string, key string) (val []byte, err error) {
	val, _, err = q.helper.getState(ns, key)
	return
}

// GetStateMetadata implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) GetStateMetadata(namespace, key string) (map[string][]byte, error) {
	return q.helper.getStateMetadata(namespace, key)
}

// GetStateMultipleKeys implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) GetStateMultipleKeys(namespace string, keys []string) ([][]byte, error) {
	return q.helper.getStateMultipleKeys(namespace, keys)
}

// GetStateRangeScanIterator implements method in interface `ledger.QueryExecutor`
// startKey is included in the results and endKey is excluded. An empty startKey refers to the first available key
// and an empty endKey refers to the last available key. For scanning all the keys, both the startKey and the endKey
// can be supplied as empty strings. However, a full scan shuold be used judiciously for performance reasons.
func (q *SnapshotBasedQueryExecutor) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (commonledger.ResultsIterator, error) {
	return q.helper.getStateRangeScanIterator(namespace, startKey, endKey)
}

// GetStateRangeScanIteratorWithMetadata implements method in interface `ledger.QueryExecutor`
// startKey is included in the results and endKey is excluded. An empty startKey refers to the first available key
// and an empty endKey refers to the last available key. For scanning all the keys, both the startKey and the endKey
// can be supplied as empty strings. However, a full scan shuold be used judiciously for performance reasons.
// metadata is a map of additional query parameters
func (q *SnapshotBasedQueryExecutor) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	return q.helper.getStateRangeScanIteratorWithMetadata(namespace, startKey, endKey, metadata)
}

// ExecuteQuery implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) ExecuteQuery(namespace, query string) (commonledger.ResultsIterator, error) {
	return q.helper.executeQuery(namespace, query)
}

// ExecuteQueryWithMetadata implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	return q.helper.executeQueryWithMetadata(namespace, query, metadata)
}

// GetPrivateData implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) GetPrivateData(namespace, collection, key string) ([]byte, error) {
	return q.helper.getPrivateData(namespace, collection, key)
}

func (q *SnapshotBasedQueryExecutor) GetPrivateDataHash(namespace, collection, key string) ([]byte, error) {
	valueHash, _, err := q.helper.getPrivateDataValueHash(namespace, collection, key)
	return valueHash, err
}

// GetPrivateDataMetadata implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) GetPrivateDataMetadata(namespace, collection, key string) (map[string][]byte, error) {
	return q.helper.getPrivateDataMetadata(namespace, collection, key)
}

// GetPrivateDataMetadataByHash implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) GetPrivateDataMetadataByHash(namespace, collection string, keyhash []byte) (map[string][]byte, error) {
	return q.helper.getPrivateDataMetadataByHash(namespace, collection, keyhash)
}

// GetPrivateDataMultipleKeys implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) GetPrivateDataMultipleKeys(namespace, collection string, keys []string) ([][]byte, error) {
	return q.helper.getPrivateDataMultipleKeys(namespace, collection, keys)
}

// GetPrivateDataRangeScanIterator implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (commonledger.ResultsIterator, error) {
	return q.helper.getPrivateDataRangeScanIterator(namespace, collection, startKey, endKey)
}

// ExecuteQueryOnPrivateData implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) ExecuteQueryOnPrivateData(namespace, collection, query string) (commonledger.ResultsIterator, error) {
	return q.helper.executeQueryOnPrivateData(namespace, collection, query)
}

// Done implements method in interface `ledger.QueryExecutor`
func (q *SnapshotBasedQueryExecutor) Done() {
	logger.Debugf("Done with transaction simulation / query execution [%s]", q.txid)
	q.helper.done()
}
