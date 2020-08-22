package fppscheduler

import (
	"time"

	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter.scheduler")

const maxUniqueKeys = 65563

type TxnScheduler struct {
	maxTxnCount   uint32
	invalid       []bool
	keyVersionMap map[uint32]*kvrwset.Version
	keyTxMap      map[uint32][]int32

	txReadSet  [][]uint64
	txWriteSet [][]uint64

	uniqueKeyCounter uint32
	uniqueKeyMap     map[string]uint32
	pendingTxns      []string
}

func NewTxnScheduler(blkSize uint32) scheduler.Scheduler {

	return &TxnScheduler{
		maxTxnCount: blkSize,

		txReadSet:  make([][]uint64, blkSize),
		txWriteSet: make([][]uint64, blkSize),

		invalid:       make([]bool, blkSize),
		keyVersionMap: make(map[uint32]*kvrwset.Version),
		keyTxMap:      make(map[uint32][]int32),

		uniqueKeyCounter: 0,
		uniqueKeyMap:     make(map[string]uint32),

		pendingTxns: make([]string, 0),
	}
}

func (scheduler *TxnScheduler) ScheduleTxn(resppayload *peer.ChaincodeAction, nextCommittedHeight uint64, txnID string) bool {

	readKeys := []string{}
	writeKeys := []string{}
	defer func(start time.Time) {
		elapsed := time.Since(start).Nanoseconds() / 1000
		logger.Infof("Process txn with read keys %v and write keys %v in %d us", readKeys, writeKeys, elapsed)
	}(time.Now())

	readSet := make([]uint64, maxUniqueKeys/64)
	writeSet := make([]uint64, maxUniqueKeys/64)
	// get current transaction id
	var err error
	txRWSet := &rwsetutil.TxRwSet{}
	if err = txRWSet.FromProtoBytes(resppayload.Results); err != nil {
		logger.Panicf("from proto bytes error")
	}
	tid := int32(len(scheduler.pendingTxns))

	ns := txRWSet.NsRwSets[1]

	// generate key for each key in the read and write set and use it to insert the read/write key into RW matrices

	// Can retrieve queryInfo from ns.KvRwSet.RangeQueriesInfo
	// ns := txRWSet.NsRwSets[0]
	for _, write := range ns.KvRwSet.Writes {
		writeKey := write.GetKey()
		writeKeys = append(writeKeys, writeKey)

		// check if the key exists
		key, ok := scheduler.uniqueKeyMap[writeKey]

		if ok == false {
			// if the key is not found, insert and increment
			// the key counter
			scheduler.uniqueKeyMap[writeKey] = scheduler.uniqueKeyCounter
			key = scheduler.uniqueKeyCounter
			scheduler.uniqueKeyCounter += 1
		}
		// set the respective bit in the writeSet

		if key >= maxUniqueKeys {
			// overflow of maxUniqueKeys
			// cut the block, and redo the work
			return false
		}

		index := key / 64
		writeSet[index] |= (uint64(1) << (key % 64))
	}

	for _, read := range ns.KvRwSet.Reads {
		readKey := read.GetKey()
		readVer := read.GetVersion()
		readKeys = append(readKeys, readKey)
		key, ok := scheduler.uniqueKeyMap[readKey]
		if ok == false {
			// if the key is not found, it is inserted. So increment
			// the key counter
			scheduler.uniqueKeyMap[readKey] = scheduler.uniqueKeyCounter
			key = scheduler.uniqueKeyCounter
			scheduler.uniqueKeyCounter += 1
		}

		ver, ok := scheduler.keyVersionMap[key]
		if ok {
			if ver.BlockNum == readVer.BlockNum && ver.TxNum == readVer.TxNum {
				scheduler.keyTxMap[key] = append(scheduler.keyTxMap[key], tid)
			} else {
				// It seems to abort the previous txns with for the unmatched version
				// logger.Infof("Invalidate txn %v", r.keyTxMap[key])
				for _, tx := range scheduler.keyTxMap[key] {
					scheduler.invalid[tx] = true
				}
				scheduler.keyTxMap[key] = nil
			}
		} else {
			scheduler.keyTxMap[key] = append(scheduler.keyTxMap[key], tid)
			scheduler.keyVersionMap[key] = readVer
		}

		// set the respective bit in the readSet
		if key >= maxUniqueKeys {
			// overflow of maxUniqueKeys
			// cut the block, and redo the work
			return false
		}

		index := key / 64
		readSet[index] |= (uint64(1) << (key % 64))
	}

	scheduler.txReadSet[tid] = readSet
	scheduler.txWriteSet[tid] = writeSet
	scheduler.pendingTxns = append(scheduler.pendingTxns, txnID)
	// logger.Infof("Finish Processing txn %d", tid)

	return true
}

func (scheduler *TxnScheduler) ProcessBlk(_ uint64) []string {
	var validCount, invalidCount int
	defer func(start time.Time) {
		scheduler.pendingTxns = make([]string, 0)
		scheduler.uniqueKeyCounter = 0
		scheduler.uniqueKeyMap = nil
		scheduler.uniqueKeyMap = make(map[string]uint32)

		scheduler.txReadSet = make([][]uint64, scheduler.maxTxnCount)
		scheduler.txWriteSet = make([][]uint64, scheduler.maxTxnCount)

		scheduler.invalid = make([]bool, scheduler.maxTxnCount)
		scheduler.keyVersionMap = make(map[uint32]*kvrwset.Version)
		scheduler.keyTxMap = make(map[uint32][]int32)

		elapsed := time.Since(start).Nanoseconds() / 1000
		logger.Infof("Process Blk in %d us ( %d valid txns, %d invalid txns)", elapsed, validCount, invalidCount)
	}(time.Now())
	if len(scheduler.pendingTxns) <= 1 {
		return scheduler.pendingTxns
	}

	txnCount := len(scheduler.pendingTxns)
	graph := make([][]int32, txnCount)
	invgraph := make([][]int32, txnCount)
	for i := int32(0); i < int32(txnCount); i++ {
		graph[i] = make([]int32, 0, txnCount)
		invgraph[i] = make([]int32, 0, txnCount)
	}

	// for every transactions, find the intersection between the readSet and the writeSet
	start := time.Now()
	for i := int32(0); i < int32(txnCount); i++ {
		for j := int32(0); j < int32(txnCount); j++ {
			if i == j || scheduler.invalid[i] || scheduler.invalid[j] {
				continue
			} else {
				for k := uint32(0); k < (maxUniqueKeys / 64); k++ {
					if (scheduler.txWriteSet[i][k] & scheduler.txReadSet[j][k]) != 0 {
						// Txn j must be scheduled before txn i
						graph[i] = append(graph[i], j)
						invgraph[j] = append(invgraph[j], i)
						break
					}
				}
			}
		}
	}
	elapsedDependency := time.Since(start).Nanoseconds() / 1000
	logger.Infof("Resolve in-blk txn dependency in %d us", elapsedDependency)

	start = time.Now()
	resGen := NewResolver(&graph, &invgraph)

	res, _ := resGen.GetSchedule()
	lenres := len(res)
	elapsedSchedule := time.Since(start).Nanoseconds() / 1000
	logger.Infof("Schedule txns in %d ", elapsedSchedule)

	resGen = nil
	graph = nil
	invgraph = nil

	validBatch := make([]string, 0)

	for i := 0; i < lenres; i++ {
		validBatch = append(validBatch, scheduler.pendingTxns[res[lenres-1-i]])
	}

	validCount = lenres
	invalidCount = 0
	for _, valid := range scheduler.invalid {
		if valid {
			invalidCount++
		}
	}
	// log some information
	// logger.Debugf("schedule-> %v", res)
	// logger.Infof("oldBlockSize:%d, newBlockSize:%d", len(r.pendingBatch), len(validBatch))
	// logger.Infof("Finish processing blk ")
	return validBatch
}
