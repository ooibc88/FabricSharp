package standardscheduler

import (
	"fmt"
	"os"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler/common"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter.scheduler")

const (
	OutAntiRw = iota
	InAntiRw
	OutCRw
	InCRw
	NA
)

type TxnType int

func (t TxnType) String() string {
	return []string{"OutAntiRw", "InAntiRw", "OutCRw", "InCRw", "NA"}[t]
}

func TxnTypeFromStr(s string) TxnType {
	ss := map[string]TxnType{"OutAntiRw": OutAntiRw, "InAntiRw": InAntiRw, "OutCRw": OutCRw, "InCRw": InCRw, "NA": NA}
	return ss[s]
}

type TxnScheduler struct {
	debug bool

	store            *common.Mvstore
	pendingWriteTxns map[string][]string
	pendingReadTxns  map[string][]string

	pendingTxns  []string
	pendingTypes map[string]TxnType
}

func NewTxnScheduler() scheduler.Scheduler {
	storePath := localconfig.MustGetStoragePath()
	if err := os.RemoveAll(storePath); err != nil {
		panic("Fail to remove " + storePath + " with err msg " + err.Error())
	}

	return &TxnScheduler{debug: true,
		store:            common.NewMvStore(storePath),
		pendingWriteTxns: make(map[string][]string),
		pendingReadTxns:  make(map[string][]string),
		pendingTxns:      make([]string, 0),
		pendingTypes:     make(map[string]TxnType)}
}

func txnTypeKey(txnID string) string {
	return fmt.Sprintf("%s_%s", "TYPE", txnID)
}

func (scheduler *TxnScheduler) getTxnType(txnID string) TxnType {
	if txnType, ok := scheduler.pendingTypes[txnID]; ok {
		return txnType
	}
	if val := scheduler.store.Get(txnTypeKey(txnID)); val != "" {
		return TxnTypeFromStr(val)
	} else {
		return NA
	}
}

func (scheduler *TxnScheduler) commitTxnTypes() {
	updates := map[string]string{}
	for txnID, txnType := range scheduler.pendingTypes {
		updates[txnTypeKey(txnID)] = txnType.String()
	}
	if err := scheduler.store.BatchUpdate(updates); err != nil {
		panic("Fail to batch update txn types...")
	}
}

func (scheduler *TxnScheduler) ProcessTxn(readSets, writeSets []string, snapshot, nextCommittedHeight uint64,
	txnID string) bool {
	action := ""
	defer func(start time.Time) {
		// do something here after surrounding function returns
		elapsed := time.Since(start).Nanoseconds() / 1000
		logger.Infof("Action: %s, Process Txn: %d us", action, elapsed)
	}(time.Now())

	voidFilter := func(s string) bool {
		return true
	}
	cRW := common.NewTxnSet()
	for _, writeKey := range writeSets {
		if _, existsPendingCww := scheduler.pendingWriteTxns[writeKey]; existsPendingCww {
			action = "CWW-Abort Txn"
			return false
		}
		if scheduler.store.UpdatedTxnsNoEarlierThanBlk(snapshot+1, writeKey, voidFilter).Size() > 0 {
			action = "CWW-Abort Txn"
			return false
		}

		if pendingReadTxns, existsPendingCrw := scheduler.pendingReadTxns[writeKey]; existsPendingCrw {
			cRW.Add(pendingReadTxns...)
		}

		committedReadTxns := scheduler.store.ReadTxnsNoEarlierThanBlk(snapshot+1, writeKey)
		cRW.Add(committedReadTxns.ToSlice()...)
	} // end for

	antiRW := common.NewTxnSet()
	freshReadKeys := []string{} // this read key does exhibit antiRw
	for _, readKey := range readSets {
		freshRead := true
		if pendingWriteTxns, existsPendingAntiRw := scheduler.pendingWriteTxns[readKey]; existsPendingAntiRw {
			antiRW.Add(pendingWriteTxns...)
			freshRead = false
		}
		committedWriteTxns := scheduler.store.UpdatedTxnsNoEarlierThanBlk(snapshot+1, readKey, voidFilter).ToSlice()
		if 0 < len(committedWriteTxns) {
			antiRW.Add(committedWriteTxns...)
			freshRead = false
		}
		if freshRead {
			freshReadKeys = append(freshReadKeys, readKey)
		}
	}

	if 0 < antiRW.Size() && 0 < cRW.Size() {
		action = "Middle-Abort Txn"
		return false
	}
	var txnType TxnType
	for _, txn := range antiRW.ToSlice() {
		if scheduler.getTxnType(txn) == OutAntiRw || scheduler.getTxnType(txn) == OutCRw {
			action = "AntiRw-Abort Txn"
			return false
		}
		txnType = OutAntiRw
	}
	for _, txn := range cRW.ToSlice() {
		if scheduler.getTxnType(txn) == InAntiRw {
			action = "RW-Abort Txn"
			return false
		}
		txnType = InCRw
	}

	scheduler.pendingTxns = append(scheduler.pendingTxns, txnID)
	for _, writeKey := range writeSets {
		if _, ok := scheduler.pendingWriteTxns[writeKey]; !ok {
			scheduler.pendingWriteTxns[writeKey] = make([]string, 0)
		}
		scheduler.pendingWriteTxns[writeKey] = append(scheduler.pendingWriteTxns[writeKey], txnID)
	}

	for _, readKey := range freshReadKeys {
		if _, ok := scheduler.pendingReadTxns[readKey]; !ok {
			scheduler.pendingReadTxns[readKey] = make([]string, 0)
		}
		scheduler.pendingReadTxns[readKey] = append(scheduler.pendingReadTxns[readKey], txnID)
	}
	scheduler.pendingTypes[txnID] = txnType
	// Note: this action may update previous txn type from InCRw to OutCRw.
	// This is ok as none of future txns may create the pattern with txn typed with InCRw.

	// Morever, the following action may update previous txn type from OutAntiRw to OutCRw.
	// This is also fine, as both conditions are inter-changable given the above disjunction for the pattern condition.
	for _, txn := range antiRW.ToSlice() {
		scheduler.pendingTypes[txn] = InAntiRw
	}
	for _, txn := range cRW.ToSlice() {
		scheduler.pendingTypes[txn] = OutCRw
	}
	action = "Schedule Txn"
	return true

}

func (scheduler *TxnScheduler) ProcessBlk(blkHeight uint64) []string {
	// Dump all
	// Clear all the ds
	schedule := make([]string, len(scheduler.pendingTxns))
	copy(schedule, scheduler.pendingTxns)
	scheduler.pendingTxns = make([]string, 0)

	scheduler.store.Commit(blkHeight, scheduler.pendingWriteTxns, scheduler.pendingReadTxns)
	scheduler.pendingReadTxns = make(map[string][]string)
	scheduler.pendingWriteTxns = make(map[string][]string)

	scheduler.commitTxnTypes()
	scheduler.pendingTypes = make(map[string]TxnType)

	return schedule
}
