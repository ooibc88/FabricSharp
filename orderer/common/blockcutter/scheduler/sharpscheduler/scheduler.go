package sharpscheduler

import (
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler/common"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
)

// type blkHeight uint64
var logger = flogging.MustGetLogger("orderer.common.blockcutter.scheduler")

type TxnScheduler struct {
	maxTxnBlkSpan uint64
	debug         bool

	store            *common.Mvstore
	pendingWriteTxns map[string]common.TxnSet
	pendingReadTxns  map[string]common.TxnSet

	// The following four fields only involve txns that may participant in the cycle
	// the first key is the starting blk height of txns in bloomfilter
	// the second key is the txn identifier
	// the value bloomfilter encapsulates all the txnID that can reach the txn identifier
	antiReachables     map[string]*common.RelayedFilter
	txnCommittedHeight map[string]uint64
	succTxns           map[string][]string
	txnAges            *common.TxnPQueue

	graph       *common.Graph     // for pending txns only
	txnSnapshot map[string]uint64 // used to calculate txn span
}

func NewTxnScheduler() scheduler.Scheduler {
	storePath := localconfig.MustGetStoragePath()
	if err := os.RemoveAll(storePath); err != nil {
		panic("Fail to remove " + storePath + " with err msg " + err.Error())
	}
	return createTxnScheduler(localconfig.GetFSharpTxnSpanLimitWithDefault(), false, storePath)
}

func createTxnScheduler(maxTxnSpan uint64, detailedLog bool, storagePath string) *TxnScheduler {
	return &TxnScheduler{
		maxTxnBlkSpan:    maxTxnSpan,
		debug:            false,
		store:            common.NewMvStore(storagePath),
		pendingWriteTxns: make(map[string]common.TxnSet),
		pendingReadTxns:  make(map[string]common.TxnSet),

		antiReachables:     make(map[string]*common.RelayedFilter),
		txnCommittedHeight: make(map[string]uint64),
		succTxns:           make(map[string][]string),
		txnAges:            common.NewTxnPQueue(),
		graph:              common.NewGraph(),
		txnSnapshot:        make(map[string]uint64),
	}

}

// False positive is possible
func (scheduler *TxnScheduler) reachable(fromTxn, toTxn string) bool {
	if antiReachable, ok := scheduler.antiReachables[toTxn]; ok {
		return antiReachable.Exists(fromTxn)
	}
	// if not found, toTxn has been pruned away as it can no longer participant in a cycle.
	return false
}

func (scheduler *TxnScheduler) ProcessTxn(readSets, writeSets []string, snapshot, nextCommittedHeight uint64,
	txnID string) bool {
	// First, resolve dependency
	var elapsedResolveDependency, elapsedTestAccessibility, elapsedPending int64 = 0, 0, 0
	var bfsTraversalCounter = 0
	status := "Schedule"
	defer func(start time.Time) {
		// do something here after surrounding function returns
		elapsed := time.Since(start).Nanoseconds() / 1000
		logFunc := logger.Infof
		if status == "DESERT" {
			logFunc = logger.Warnf
		}
		logFunc("%s Txn %s in %d us. (Resolve Dependency: %d us, Test accessibility via BFS traverse %d txns: %d us, Update Pending: %d us)\n\n", status, txnID, elapsed, elapsedResolveDependency, bfsTraversalCounter, elapsedTestAccessibility, elapsedPending)
	}(time.Now())

	if scheduler.maxTxnBlkSpan < nextCommittedHeight && snapshot < nextCommittedHeight-scheduler.maxTxnBlkSpan {
		status = "Drop"
		return false
	}

	startResolveDependency := time.Now()
	wr := common.NewTxnSet()
	antiRw := common.NewTxnSet()
	antiPendingRw := common.NewTxnSet()

	newReadKeys := make([]string, 0) // newly read keys without overriden before
	filter := func(txn string) bool {
		return scheduler.txnAges.Exists(txn)
	}
	for _, readKey := range readSets {
		// excluding the snapshot block,
		// a txn with snapshot s implies that it reads the states committed at s.
		curAntiRw := scheduler.store.UpdatedTxnsNoEarlierThanBlk(snapshot+1, readKey, filter)
		antiRw.InPlaceUnion(curAntiRw)
		if curWr, found := scheduler.store.LastUpdatedTxnNoLaterThanBlk(snapshot, readKey, filter); found {
			wr.Add(curWr)
		}
		existsAntiPendingRw := false
		if curPendingWriteTxns, ok := scheduler.pendingWriteTxns[readKey]; ok {
			antiPendingRw.InPlaceUnion(curPendingWriteTxns)
			existsAntiPendingRw = true
		}
		// Complete NO antiRW txns
		if len(curAntiRw) == 0 && !existsAntiPendingRw {
			newReadKeys = append(newReadKeys, readKey)
		}
	} // end for readKey

	ww := common.NewTxnSet()
	rw := common.NewTxnSet()

	for _, writeKey := range writeSets {
		// do not consider to ww conflicts with the pending txns, as they are all reorderable
		// committed ww-conflict txns may not exist, as they may already be pruned away due to the non-concurrency
		if curWw, found := scheduler.store.LastUpdatedTxnNoLaterThanBlk(nextCommittedHeight, writeKey, filter); found {
			ww.Add(curWw)
		}

		curRw := scheduler.store.ReadTxnsEarlierThanBlk(nextCommittedHeight, writeKey, filter)
		rw.InPlaceUnion(curRw)
		if curPendingRw, ok := scheduler.pendingReadTxns[writeKey]; ok {
			rw.InPlaceUnion(curPendingRw)
		}
	}

	logger.Infof("Txn %s Dependency: wr=%s, ww=%s, rw=%s, anti-rw=%s, pending-anti-rw=%s, freshly read keys=%v", txnID, wr, ww, rw, antiRw, antiPendingRw, newReadKeys)
	elapsedResolveDependency = time.Since(startResolveDependency).Nanoseconds() / 1000

	// Update the accessibility info and detect cycle in the meantime
	startTestAccessibility := time.Now()
	curPredTxns := common.NewTxnSet()
	curPredTxns.InPlaceUnion(wr)
	curPredTxns.InPlaceUnion(rw)
	curPredTxns.InPlaceUnion(ww)

	curSuccTxns := common.NewTxnSet()
	curSuccTxns.InPlaceUnion(antiRw)
	curSuccTxns.InPlaceUnion(antiPendingRw)

	// fmt.Printf("Out txns: %v\n", outgoings.internal)
	for curSucc := range curSuccTxns {
		for curPred := range curPredTxns {
			if scheduler.reachable(curSucc, curPred) {
				logger.Infof("Cycle detected for %s from %s to %s", txnID, curSucc, curPred)
				status = "Abort"
				return false
			}
		}
	}

	// Only compute txn span for committed, rw txns (excluding write-only txn)
	if len(readSets) > 0 {
		scheduler.txnSnapshot[txnID] = snapshot
	}

	scheduler.antiReachables[txnID] = common.CreateRelayedFilter(nextCommittedHeight)
	scheduler.antiReachables[txnID].Add(txnID)
	// Update outgoing link for incoming txns
	for curPredTxn := range curPredTxns {
		// curPredTxn may be too stale to be included in the dependency graph.
		if _, ok := scheduler.succTxns[curPredTxn]; ok {
			scheduler.succTxns[curPredTxn] = append(scheduler.succTxns[curPredTxn], txnID)
		}

		if _, ok := scheduler.antiReachables[curPredTxn]; ok {
			scheduler.antiReachables[txnID].Merge(scheduler.antiReachables[curPredTxn])
		}
	}

	scheduler.succTxns[txnID] = curSuccTxns.ToSlice()

	// Union the antiAccessible set for all txns during bfs with that of txnID
	// Update the txnQ accordingly
	bfsTxn := []string{txnID}
	bfsVisited := map[string]bool{}
	for len(bfsTxn) > 0 {
		bfsTraversalCounter++
		curTxn := bfsTxn[0]
		bfsTxn = bfsTxn[1:]
		scheduler.txnAges.Push(curTxn, nextCommittedHeight)
		scheduler.antiReachables[curTxn].Merge(scheduler.antiReachables[txnID])

		// fmt.Printf("curTxn: %s, BFS txns: %v\n", curTxn, bfsTxn)
		for _, outTxn := range scheduler.succTxns[curTxn] {
			if _, exists := bfsVisited[outTxn]; !exists {
				bfsTxn = append(bfsTxn, outTxn)
				bfsVisited[outTxn] = true
			} // end if
		} // end for
	}

	elapsedTestAccessibility = time.Since(startTestAccessibility).Nanoseconds() / 1000

	// Update the relevant fields
	startIndexing := time.Now()
	edgesFrom := []string{}
	edgesTo := []string{}
	for _, pendingTxn := range scheduler.graph.Nodes() {
		if scheduler.reachable(pendingTxn, txnID) && scheduler.reachable(txnID, pendingTxn) {
			// Theoertically, such txn cycle is impossbile due to the fact that
			// all txn dependency have been resolved previously.
			// However, this is practically possible due to the false positive from bloom filter.
			// In this case, we have to desert this valid txn from the schedule as we dont know which depedency direction is wrong.
			// For convenience, we still retain this txn in the dependency graph for later txns, which may introduce overhead.
			// This scenario SHOULD be minimized by lowering down the false positive rate of bloom filter.

			status = "DESERT"
			return false
		} else if scheduler.reachable(pendingTxn, txnID) {
			edgesFrom = append(edgesFrom, pendingTxn)
			edgesTo = append(edgesTo, txnID)
		} else if scheduler.reachable(txnID, pendingTxn) {
			edgesFrom = append(edgesFrom, txnID)
			edgesTo = append(edgesTo, pendingTxn)
		}
	}

	if ok := scheduler.graph.AddNode(txnID); !ok {
		logger.Errorf("Node %s has been added before...", txnID)
	}
	for i := range edgesFrom {
		from := edgesFrom[i]
		to := edgesTo[i]
		if ok := scheduler.graph.AddEdge(from, to); !ok {
			logger.Errorf("Edge %s -> %s has been added before", from, to)
		}
	}

	for _, writeKey := range writeSets {
		if _, found := scheduler.pendingWriteTxns[writeKey]; !found {
			scheduler.pendingWriteTxns[writeKey] = common.NewTxnSet()
		}
		scheduler.pendingWriteTxns[writeKey].Add(txnID)
	}

	for _, newReadKey := range newReadKeys {
		if _, found := scheduler.pendingReadTxns[newReadKey]; !found {
			scheduler.pendingReadTxns[newReadKey] = common.NewTxnSet()
		}
		scheduler.pendingReadTxns[newReadKey].Add(txnID)
	}
	elapsedPending = time.Since(startIndexing).Nanoseconds() / 1000
	return true
}

func (scheduler *TxnScheduler) pruneFilters(curBlkHeight uint64) {
	// All the considered txns in the dependency graph are committed no earlier than earliestCommittedBlk
	var earliestCommittedBlk uint64 = math.MaxUint64
	for _, committedBlk := range scheduler.txnCommittedHeight {
		if committedBlk < earliestCommittedBlk {
			earliestCommittedBlk = committedBlk
		}
	}

	for _, antiReachable := range scheduler.antiReachables {
		antiReachable.Rotate(earliestCommittedBlk, curBlkHeight)
	}
}

const filterInterval = uint64(40)

func (scheduler *TxnScheduler) ProcessBlk(blkHeight uint64) []string {
	txnQueueRemovalCount := 0
	topoTraversalCounter := 0
	var elapsedPrunePQueue, elapsedResolvePendingTxn, elapsedTopoTraverseForWw, elapsedStorage, elapsedTopoSort int64 = 0, 0, 0, 0, 0
	avgSpan := 0.0
	defer func(start time.Time) {
		// do something here after surrounding function returns
		elapsed := time.Since(start).Nanoseconds() / 1000
		// Concerned txns refer to txns may participant in the cycle, despite of tis committed status or not
		// logger.Infof("Retrieve Schedule for block %d ( average spanned = %.2f ) in %d us. (bfs traverse concerned txns to update last anti-dependent blk in %d us, prune %d concerned txns from txnQueue in %d us, resolve pending txns for schedule in %d us, dump scheduled txns to storage in %d us and prune filters in %d us", committed, avgSpan, elapsed, elapsedBFS, txnQueueRemovalCount, elapsedPrunePQueue, elapsedResolvePendingTxn, elapsedStorage, elapsedPruneFilter)
		logger.Infof("Retrieve Schedule for block %d ( average spanned = %.2f ) in %d us.", blkHeight, avgSpan, elapsed)
		logger.Infof("Compute schedule from topo sort in %d us", elapsedTopoSort)
		logger.Infof("Resolve pending txns in %d us", elapsedResolvePendingTxn)
		logger.Infof("Store committed keys in %d us", elapsedStorage)
		logger.Infof("Topo Traverse %d txns to update ww dependency in %d us", topoTraversalCounter, elapsedTopoTraverseForWw)
		logger.Infof("Remove %d non-concerned txns from TxnPQueue in %d us", txnQueueRemovalCount, elapsedPrunePQueue)
		logger.Infof("# of Concerned txns in the queue : %d", scheduler.txnAges.Size())
	}(time.Now())

	// Ignore committed snapshot txns which can not be concurrent ( and hence reverse dependent to form  a cycle)
	// plus ONE as the earliest snapshot applies to txns in the later blocks.
	earliestSnapshot := int64(blkHeight) + 1 - int64(scheduler.maxTxnBlkSpan)

	// TODO: Temporally not to remove older versions of records, as it is too expensive to iterate all records. Future optimization may do it asyncly.
	// scheduler.store.Clean(uint64(earliestSnapshot))

	// logger.Infof("Start STAGE %d for Blk %d", 1, committed)
	start := time.Now()
	var schedule []string
	var ok bool
	// Cycle is possible, as the edge could be wrongly added due to false positive
	// If cycle happends, remove all txns participanting in the cycle
	if schedule, ok = scheduler.graph.Toposort(); !ok {
		panic("Cycle detected...")
	}

	if scheduler.debug {
		for i, txn1 := range schedule {
			for j, txn2 := range schedule {
				if i < j && scheduler.reachable(txn2, txn1) {
					logger.Infof("Schedule: %v", schedule)
					panic("Invalid schedule!!!!!!" + txn2 + " " + txn1)
				}
			}
		}
	}

	totalTxnblkSpan := uint64(0)
	count := uint64(0)
	schedulePosition := make(map[string]int)
	for idx, txnID := range schedule {
		schedulePosition[txnID] = idx
		if snapshot, ok := scheduler.txnSnapshot[txnID]; ok {
			count++
			totalTxnblkSpan += blkHeight - snapshot
		}

		scheduler.txnCommittedHeight[txnID] = blkHeight
	}
	elapsedTopoSort = time.Since(start).Nanoseconds() / 1000

	// logger.Infof("Start STAGE %d for Blk %d", 2, committed)
	start = time.Now()
	topoTraverseStartTxns := []string{}
	committedUpdates, committedReads := map[string][]string{}, map[string][]string{}
	for writeKey, pendingTxns := range scheduler.pendingWriteTxns {
		sortedWriteTxns := pendingTxns.ToSlice()
		// sort the txns in pendingWrite based on the relative order in schedule.
		sort.Slice(sortedWriteTxns, func(i, j int) bool {
			return schedulePosition[sortedWriteTxns[i]] < schedulePosition[sortedWriteTxns[j]]
		})

		committedUpdates[writeKey] = sortedWriteTxns
		// This key will be updated at this block
		// These txns can not read the recent committed records,
		//   as the new record is committed.
		delete(scheduler.pendingReadTxns, writeKey)

		if len(sortedWriteTxns) > 1 {
			// Take into account of the in-block ww dependency
			pendingSize := len(sortedWriteTxns)
			findHeadTxn := false
			for i := 1; i < pendingSize; i++ {
				predTxn := sortedWriteTxns[i-1]
				succTxn := sortedWriteTxns[i]
				// Update the succTxns of the dependency graph here for each ww.
				scheduler.succTxns[predTxn] = append(scheduler.succTxns[predTxn], succTxn)
				if scheduler.reachable(succTxn, predTxn) {
					panic("From the topo sort, this ww dependency shall not create a cycle...")
				}
				// tx1 -ww-> tx2 -ww-> tx3 -ww-> tx4
				// Find the first succ tx whose ww dependency has not been incorporated in anti-reachable.
				// Update its anti_reachable from its predecessor and record down this succ txn into topoTraverseStartTxns.
				// Later, we update the antiReachable of all txns accessible from topoTraverseStartTxns in a single iteration.

				// The rationale is that suppose tx1 -~-> tx2 already holds without considering their ww dependency, we do not need to consider this ww for later iteration.
				if !findHeadTxn && !scheduler.reachable(predTxn, succTxn) {
					findHeadTxn = true
					topoTraverseStartTxns = append(topoTraverseStartTxns, succTxn)
					scheduler.antiReachables[succTxn].Merge(scheduler.antiReachables[predTxn])
				}
			} // end for
		} // end if len
	}
	// clear the pendingWriteTxns
	scheduler.pendingWriteTxns = make(map[string]common.TxnSet)

	for readKey, pendingTxns := range scheduler.pendingReadTxns {
		committedReads[readKey] = pendingTxns.ToSlice()
	}

	scheduler.pendingReadTxns = make(map[string]common.TxnSet)
	elapsedResolvePendingTxn = time.Since(start).Nanoseconds() / 1000

	///////////////////////////////////////////////////////////////////
	// Associate txns and keys in the storage.
	// logger.Infof("Start STAGE %d for Blk %d", 4, committed)
	start = time.Now()
	scheduler.store.Commit(blkHeight, committedUpdates, committedReads)
	elapsedStorage = time.Since(start).Nanoseconds() / 1000

	///////////////////////////////////////////////////////////////////
	// Topo-sorted Traverse relevant txns to propagate ww dependency for accessibility information.
	// logger.Infof("Start STAGE %d for Blk %d", 5, committed)
	start = time.Now()
	// So that all ww-dependency can be considered.
	sort.Slice(topoTraverseStartTxns, func(i, j int) bool {
		return schedulePosition[topoTraverseStartTxns[i]] < schedulePosition[topoTraverseStartTxns[j]]
	})
	topoTraversalCounter = scheduler.topoTraverseForWw(topoTraverseStartTxns)
	elapsedTopoTraverseForWw = time.Since(start).Nanoseconds() / 1000
	/////////////////////////////////////////////////////////////////////////////
	// remove txns from txnQ which can not be concurrent ( and hence reverse dependent)
	// logger.Infof("Start STAGE %d for Blk %d", 6, committed)
	start = time.Now()
	prevTxn, depHeight, found := scheduler.txnAges.Peek()
	removedTxnDepMap := make(map[string]uint64)
	for found && int64(depHeight) <= earliestSnapshot {
		scheduler.txnAges.Pop()
		removedTxnDepMap[prevTxn] = depHeight

		delete(scheduler.txnCommittedHeight, prevTxn)
		delete(scheduler.succTxns, prevTxn)
		delete(scheduler.antiReachables, prevTxn)

		prevTxn, depHeight, found = scheduler.txnAges.Peek()
		txnQueueRemovalCount++
	}
	elapsedPrunePQueue = time.Since(start).Nanoseconds() / 1000

	if scheduler.debug {
		logger.Infof("Remove Txns from PQueue : %v", removedTxnDepMap)
	}

	/////////////////////////////////////////////////////////////////
	// Create new set of filters for every filterInterval blocks
	scheduler.pruneFilters(blkHeight)
	avgSpan = float64(totalTxnblkSpan) / float64(count)
	return schedule
}

func (scheduler *TxnScheduler) topoTraverseForWw(startTxns []string) (counter int) {
	counter = 0
	topoOrder := common.TopoOrder(startTxns, func(from string) []string {
		return scheduler.succTxns[from]
	})
	// Propogate the ww dependency based on the computed topo order
	for _, curTxn := range topoOrder {
		for _, succTxn := range scheduler.succTxns[curTxn] {
			scheduler.antiReachables[succTxn].Merge(scheduler.antiReachables[curTxn])
		} // for outTxn
	} // for i
	counter = len(topoOrder)
	return
}

func (scheduler *TxnScheduler) reportStats(curBlkHeight uint64) {
	pendingUpdateRecordCount := 0
	pendingUpdateTxnCount := 0
	for _, pendingTxns := range scheduler.pendingWriteTxns {
		pendingUpdateRecordCount++
		pendingUpdateTxnCount += len(pendingTxns)
	}

	pendingReadRecordCount := 0
	pendingReadTxnCount := 0
	for _, pendingTxns := range scheduler.pendingReadTxns {
		pendingReadRecordCount++
		pendingReadTxnCount += len(pendingTxns)
	}

	nodeCount, edgeCount := scheduler.graph.Size()
	totalTxnCount := scheduler.txnAges.Size()

	logger.Infof("Current Blk Height: %d "+
		"pendingUpdate: %d keys, %d txns. "+
		"pendingRead: %d keys, %d txns. "+
		"Graph: %d nodes, %d edges. "+
		"Total Concerned Txns: %d",
		curBlkHeight,
		pendingUpdateRecordCount, pendingUpdateTxnCount,
		pendingReadRecordCount, pendingReadTxnCount,
		nodeCount, edgeCount, totalTxnCount)
}

func (scheduler *TxnScheduler) detailedStat(curBlkHeight uint64) {
	fmt.Printf("Current Block Height: %d\n", curBlkHeight)
	fmt.Printf("pendingWriteTxns: %v\n", scheduler.pendingWriteTxns)
	fmt.Printf("pendingReadTxns: %v\n", scheduler.pendingReadTxns)

	fmt.Printf("Txn Queue: %v\n", scheduler.txnAges.Raw())
}
