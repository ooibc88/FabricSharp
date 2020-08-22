package fifoscheduler

import (
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter.scheduler")

type TxnScheduler struct {
	pendingTxns []string
}

func NewTxnScheduler() scheduler.Scheduler {

	return &TxnScheduler{
		pendingTxns: make([]string, 0),
	}
}

func (scheduler *TxnScheduler) ScheduleTxn(resppayload *peer.ChaincodeAction, nextCommittedHeight uint64, txnID string) bool {
	scheduler.pendingTxns = append(scheduler.pendingTxns, txnID)
	return true
}

func (scheduler *TxnScheduler) ProcessBlk(blkHeight uint64) []string {
	schedule := scheduler.pendingTxns
	scheduler.pendingTxns = make([]string, 0)

	return schedule
}
