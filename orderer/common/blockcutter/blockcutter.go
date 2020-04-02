/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockcutter

import (
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter")

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

// Receiver defines a sink for the ordered broadcast messages
type Receiver interface {
	// Ordered should be invoked sequentially as messages are ordered
	// Each batch in `messageBatches` will be wrapped into a block.
	// `pending` indicates if there are still messages pending in the receiver.
	Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool)

	// Cut returns the current batch and starts a new one
	Cut() []*cb.Envelope
}

type receiver struct {
	sharedConfigFetcher   OrdererConfigFetcher
	pendingBatch          map[string]*cb.Envelope
	pendingBatchSizeBytes uint32

	PendingBatchStartTime time.Time
	ChannelID             string
	Metrics               *Metrics

	scheduler                 scheduler.Scheduler
	blkHeight                 uint64
	pendingNonEndorseTxnBatch []*cb.Envelope
}

// NewReceiverImpl creates a Receiver implementation based on the given configtxorderer manager
func NewReceiverImpl(channelID string, sharedConfigFetcher OrdererConfigFetcher, scheduler scheduler.Scheduler, metrics *Metrics) Receiver {
	return &receiver{
		sharedConfigFetcher:       sharedConfigFetcher,
		Metrics:                   metrics,
		pendingBatch:              make(map[string]*cb.Envelope),
		pendingBatchSizeBytes:     0,
		ChannelID:                 channelID,
		scheduler:                 scheduler,
		blkHeight:                 uint64(1), // due to the presence of genesis block, block height starts from 1.
		pendingNonEndorseTxnBatch: make([]*cb.Envelope, 0),
	}
}

func validKey(key string) bool {
	// The keys without the following suffix have special meanings.
	// And should be ignored for reordering.
	if strings.HasSuffix(key, "_prov") {
		return false
	}

	if strings.HasSuffix(key, "_txnID") {
		return false
	}

	if strings.HasSuffix(key, "_hist") {
		return false
	}

	if strings.HasSuffix(key, "_forward") {
		return false
	}

	if strings.HasSuffix(key, "_backward") {
		return false
	}

	return true
}

func (r *receiver) scheduleMsg(msg *cb.Envelope) bool {
	var err error
	var txnID string
	data := make([]byte, messageSizeBytes(msg))
	if data, err = proto.Marshal(msg); err != nil {
		panic("Can not marshal the txn")
	}
	// var payload *cb.Payload
	if payload, err := utils.GetPayload(msg); err != nil {
		panic("Can not get payload from the txn envelop")
	} else if chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader); err != nil {
		panic("Can not marshal channel header from the txn payload")
	} else if cb.HeaderType(chdr.Type) != cb.HeaderType_ENDORSER_TRANSACTION {
		txnID = chdr.TxId[0:8] // only select the 8 char prefix
		r.pendingNonEndorseTxnBatch = append(r.pendingNonEndorseTxnBatch, msg)
		logger.Infof("Put ahead non-endorsement txn %s\n\n", txnID)
		return true
	} else {
		txnID = chdr.TxId[0:8] // only select the 8 char prefix
	}

	var resppayload *peer.ChaincodeAction
	if resppayload, err = utils.GetActionFromEnvelope(data); err != nil {
		panic("Fail to get action from the txn envelop")
	}

	txRWSet := &rwsetutil.TxRwSet{}
	if err := txRWSet.FromProtoBytes(resppayload.Results); err != nil {
		panic("Fail to retrieve rwset from txn payload")
	}

	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)

	readSnapshot := r.blkHeight // if there are no read keys, readSnapshot is useless. Just set it to the next block height
	ns := txRWSet.NsRwSets[0]   // the actual read/write keys is located in the first namespace.
	for _, write := range ns.KvRwSet.Writes {
		if writeKey := write.GetKey(); validKey(writeKey) {
			writeKeys = append(writeKeys, writeKey)
		}
	}

	for _, read := range ns.KvRwSet.Reads {
		if readKey := read.GetKey(); validKey(readKey) {
			// all read key versions should have the same block number, as they are retrieved from the identical snapshot
			readSnapshot = read.GetVersion().GetBlockNum()
			readKeys = append(readKeys, readKey)
		}
	}

	logger.Infof("Process msg %s: snapshot=%d, committed=%d, readKeys=%v, writeKeys=%v", txnID, readSnapshot, r.blkHeight, readKeys, writeKeys)
	if ok := r.scheduler.ProcessTxn(readKeys, writeKeys, readSnapshot, r.blkHeight, txnID); ok {
		r.pendingBatch[txnID] = msg
		return true
	} else {
		return false
	}
}

// Ordered should be invoked sequentially as messages are ordered
//
// messageBatches length: 0, pending: false
//   - the first ordered txn encounters a cycle
// messageBatches length: 0, pending: true
//   - no batch is cut and there are messages pending
// messageBatches length: 1, pending: false
//   -a large txn without any pending txns before
//   -ReorderCycle = blockSize
// messageBatches length: 1, pending: true
//   - impossible
//
// Note that messageBatches can not be greater than 2.
func (r *receiver) Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool) {
	if len(r.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		r.PendingBatchStartTime = time.Now()
	}

	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()
	blkSize := int(batchSize.MaxMessageCount)
	if b := localconfig.TryGetBlockSize(); 0 < b {
		blkSize = b
		logger.Infof("Override the current block size with env var to %d", blkSize)
	}
	messageSizeBytes := messageSizeBytes(msg)

	if messageSizeBytes+r.pendingBatchSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)

		// cut pending batch, if it has any messages
		if len(r.pendingBatch) > 0 {
			messageBatch := r.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)
	}

	if r.scheduleMsg(msg) {
		r.pendingBatchSizeBytes += messageSizeBytes
	}

	if len(r.pendingBatch) >= blkSize {
		messageBatch := r.Cut()
		messageBatches = append(messageBatches, messageBatch)
		logger.Infof("# of txns in cut batch %d, # of batches %d", len(messageBatch), len(messageBatches))
		logger.Infof("================================================================\n")
	}

	pending = len(r.pendingBatch) > 0

	return
}

// Cut returns the current batch and starts a new one
func (r *receiver) Cut() []*cb.Envelope {
	r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	r.PendingBatchStartTime = time.Time{}

	schedule := r.scheduler.ProcessBlk(r.blkHeight)

	batch := make([]*cb.Envelope, 0)
	// Put ahead any non-endorsement txn first in the batch
	batch = append(batch, r.pendingNonEndorseTxnBatch...)
	for _, txnID := range schedule {
		batch = append(batch, r.pendingBatch[txnID])
	}
	r.pendingBatchSizeBytes = 0
	r.pendingBatch = make(map[string]*cb.Envelope)
	r.blkHeight++
	return batch
}

func messageSizeBytes(message *cb.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}
