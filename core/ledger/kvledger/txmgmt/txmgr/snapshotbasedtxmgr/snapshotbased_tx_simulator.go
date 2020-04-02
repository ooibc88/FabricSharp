/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package snapshotbasedtxmgr

import (
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/pkg/errors"
)

// snapshotTxSimulator is a transaction simulator used in `LockBasedTxMgr`
type snapshotTxSimulator struct {
	SnapshotBasedQueryExecutor
	rwsetBuilder              *rwsetutil.RWSetBuilder
	writePerformed            bool
	pvtdataQueriesPerformed   bool
	simulationResultsComputed bool
	paginatedQueriesPerformed bool
}

func newSnapshotTxSimulator(txmgr *SnapshotBasedTxMgr, txid string) (*snapshotTxSimulator, error) {
	rwsetBuilder := rwsetutil.NewRWSetBuilder()
	helper := newQueryHelper(txmgr, rwsetBuilder)
	logger.Debugf("constructing new tx simulator txid = [%s]", txid)
	return &snapshotTxSimulator{SnapshotBasedQueryExecutor{helper, txid}, rwsetBuilder, false, false, false, false}, nil
}

// SetState implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) SetState(ns string, key string, value []byte) error {
	s.rwsetBuilder.AddToWriteSet(ns, key, value)
	return nil
}

// DeleteState implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) DeleteState(ns string, key string) error {
	return s.SetState(ns, key, nil)
}

// SetStateMultipleKeys implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) SetStateMultipleKeys(namespace string, kvs map[string][]byte) error {
	for k, v := range kvs {
		if err := s.SetState(namespace, k, v); err != nil {
			return err
		}
	}
	return nil
}

// SetStateMetadata implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) SetStateMetadata(namespace, key string, metadata map[string][]byte) error {
	s.rwsetBuilder.AddToMetadataWriteSet(namespace, key, metadata)
	return nil
}

// DeleteStateMetadata implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) DeleteStateMetadata(namespace, key string) error {
	return s.SetStateMetadata(namespace, key, nil)
}

// SetPrivateData implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) SetPrivateData(ns, coll, key string, value []byte) error {
	s.writePerformed = true
	s.rwsetBuilder.AddToPvtAndHashedWriteSet(ns, coll, key, value)
	return nil
}

// DeletePrivateData implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) DeletePrivateData(ns, coll, key string) error {
	return s.SetPrivateData(ns, coll, key, nil)
}

// SetPrivateDataMultipleKeys implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) SetPrivateDataMultipleKeys(ns, coll string, kvs map[string][]byte) error {
	for k, v := range kvs {
		if err := s.SetPrivateData(ns, coll, k, v); err != nil {
			return err
		}
	}
	return nil
}

// GetPrivateDataRangeScanIterator implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) GetPrivateDataRangeScanIterator(namespace, collection, startKey, endKey string) (commonledger.ResultsIterator, error) {
	panic("Not implemented here")
	return nil, nil
}

// SetPrivateDataMetadata implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) SetPrivateDataMetadata(namespace, collection, key string, metadata map[string][]byte) error {
	panic("Not implemented here")
	return nil
}

// DeletePrivateMetadata implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) DeletePrivateDataMetadata(namespace, collection, key string) error {
	panic("Not implemented here")
	return nil
}

// ExecuteQueryOnPrivateData implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) ExecuteQueryOnPrivateData(namespace, collection, query string) (commonledger.ResultsIterator, error) {
	panic("Not implemented here")
	return nil, nil
}

// GetStateRangeScanIteratorWithMetadata implements method in interface `ledger.QueryExecutor`
func (s *snapshotTxSimulator) GetStateRangeScanIteratorWithMetadata(namespace string, startKey string, endKey string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	panic("Not implemented here")
	return nil, nil
}

// ExecuteQueryWithMetadata implements method in interface `ledger.QueryExecutor`
func (s *snapshotTxSimulator) ExecuteQueryWithMetadata(namespace, query string, metadata map[string]interface{}) (ledger.QueryResultsIterator, error) {
	panic("Not implemented here")
	return nil, nil
}

// GetTxSimulationResults implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) GetTxSimulationResults() (*ledger.TxSimulationResults, error) {
	if s.simulationResultsComputed {
		return nil, errors.New("this function should only be called once on a transaction simulator instance")
	}
	defer func() { s.simulationResultsComputed = true }()
	logger.Debugf("Simulation completed, getting simulation results")
	if s.helper.err != nil {
		return nil, s.helper.err
	}
	return s.rwsetBuilder.GetTxSimulationResults()
}

// ExecuteUpdate implements method in interface `ledger.TxSimulator`
func (s *snapshotTxSimulator) ExecuteUpdate(query string) error {
	return errors.New("not supported")
}
