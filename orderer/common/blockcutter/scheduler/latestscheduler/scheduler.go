package latestscheduler

import (
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler/common"
)

var logger = flogging.MustGetLogger("orderer/common/blockcutter/scheduler")

// TxnScheduler implements the Sort-Based Greedy Algorithm in http://www.vldb.org/pvldb/vol12/p169-ding.pdf
type TxnScheduler struct {
	debug bool

	pendingWriteTxns map[string][]string
	pendingReadTxns  map[string][]string

	pendingTxns []string
}

func NewTxnScheduler() scheduler.Scheduler {
	return initTxnScheduler()
}

func initTxnScheduler() *TxnScheduler {
	return &TxnScheduler{debug: true,
		pendingWriteTxns: make(map[string][]string),
		pendingReadTxns:  make(map[string][]string),
		pendingTxns:      make([]string, 0),
	}
}

func (scheduler *TxnScheduler) ProcessTxn(readSets, writeSets []string, snapshot, nextCommittedHeight uint64,
	txnID string) bool {
	defer func(start time.Time) {
		// do something here after surrounding function returns
		elapsed := time.Since(start).Nanoseconds() / 1000
		logger.Infof("Process Txn: %d us", elapsed)
	}(time.Now())

	for _, writeKey := range writeSets {
		if _, ok := scheduler.pendingWriteTxns[writeKey]; !ok {
			scheduler.pendingWriteTxns[writeKey] = make([]string, 0)
		}
		scheduler.pendingWriteTxns[writeKey] = append(scheduler.pendingWriteTxns[writeKey], txnID)
	}

	for _, readKey := range readSets {
		if _, ok := scheduler.pendingReadTxns[readKey]; !ok {
			scheduler.pendingReadTxns[readKey] = make([]string, 0)
		}
		scheduler.pendingReadTxns[readKey] = append(scheduler.pendingReadTxns[readKey], txnID)
	}
	scheduler.pendingTxns = append(scheduler.pendingTxns, txnID)
	return true
}

func (scheduler *TxnScheduler) computeDep() (pred, succ map[string]common.TxnSet) {
	pred = make(map[string]common.TxnSet)
	succ = make(map[string]common.TxnSet)
	for readKey, readTxns := range scheduler.pendingReadTxns {
		for _, readTxn := range readTxns {
			if _, ok := succ[readTxn]; !ok {
				succ[readTxn] = common.NewTxnSet()
			}

			for _, writeTxn := range scheduler.pendingWriteTxns[readKey] {
				if readTxn != writeTxn {
					succ[readTxn].Add(writeTxn)
				}
			}
		}
	}

	for writeKey, writeTxns := range scheduler.pendingWriteTxns {
		for _, writeTxn := range writeTxns {
			if _, ok := pred[writeTxn]; !ok {
				pred[writeTxn] = common.NewTxnSet()
			}

			for _, readTxn := range scheduler.pendingReadTxns[writeKey] {
				if writeTxn != readTxn {
					pred[writeTxn].Add(readTxn)
				}
			}
		}
	}
	return
}

func (scheduler *TxnScheduler) ProcessBlk(blkHeight uint64) []string {
	dropCount := 0
	var elapsedDepResolution, elapsedPruneFilter int64
	defer func(start time.Time) {
		// do something here after surrounding function returns
		elapsed := time.Since(start).Nanoseconds() / 1000
		logger.Infof("Process Block: %d us, Resolve Dep: %d us, Prune Filter: %d us, # of dropped txns: %d", elapsed, elapsedDepResolution, elapsedPruneFilter, dropCount)
	}(time.Now())
	schedule := make([]string, 0)
	scheduleSet := common.NewTxnSet()
	dropSet := common.NewTxnSet()
	planned := func(txnID string) bool {
		return scheduleSet.Exists(txnID) || dropSet.Exists(txnID)
	}
	start := time.Now()
	pred, succ := scheduler.computeDep()
	elapsedDepResolution = time.Since(start).Nanoseconds() / 1000

	start = time.Now()

	for scheduleSet.Size()+dropSet.Size() < len(scheduler.pendingTxns) {
		// Prune phase: continuously scan the unplanned txns and schedule without predecessors.
		for true {
			scheduledTxn := ""
			for _, txn := range scheduler.pendingTxns {
				if planned(txn) {
					continue
				}
				predCount := 0
				if predTxns, ok := pred[txn]; ok {
					for predTxn := range predTxns {
						if !planned(predTxn) {
							predCount++
						}
					}
				}

				if predCount == 0 {
					scheduledTxn = txn
					break
				}
			}
			if scheduledTxn != "" {
				schedule = append(schedule, scheduledTxn)
				// logger.Infof("Schedule txn %s", scheduledTxn)
				scheduleSet.Add(scheduledTxn)
				// continue the pruning phase
			} else {
				break // the remaining schedule is acyclic, go ahead for filtering phase
			}
		}

		// Filter phase: Drop the txn with the max of the sum of the in-degree and out-degree.
		maxDegree := 0
		dropTxn := ""
		for _, txn := range scheduler.pendingTxns {
			if planned(txn) {
				continue
			}
			sumDegree := 0
			if predTxns, ok := pred[txn]; ok {
				for predTxn := range predTxns {
					if !planned(predTxn) {
						sumDegree++
					}
				}
			}

			if succTxns, ok := succ[txn]; ok {
				for succTxn := range succTxns {
					if !planned(succTxn) {
						sumDegree++
					}
				}
			}
			if maxDegree < sumDegree {
				maxDegree = sumDegree
				dropTxn = txn
			}
		}
		if dropTxn != "" {
			// logger.Infof("Drop txn %s", dropTxn)
			dropSet.Add(dropTxn)
		} else {
			// dropTxn can be nil if the previous prune phase schedule all txns
		}
	} // end for scheduleSet.Size()+dropSet.Size() < len(scheduler.pendingTxns)

	dropCount = dropSet.Size()
	elapsedPruneFilter = time.Since(start).Nanoseconds() / 1000

	// return schedule
	scheduler.pendingTxns = make([]string, 0)
	scheduler.pendingReadTxns = make(map[string][]string)
	scheduler.pendingWriteTxns = make(map[string][]string)
	return schedule
}
