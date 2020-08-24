package scheduler

import (
	"strings"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
)

type Scheduler interface {
	ScheduleTxn(resppayload *peer.ChaincodeAction, nextCommittedHeight uint64, txnID string) bool
	ProcessBlk(committedHeight uint64) []string
}

func validKey(key string) bool {
	// The keys without the following suffix have special meanings.
	// And should be ignored for reordering.
	if strings.HasSuffix(key, "initialized") {
		// If chaincode is deployed with "--init-required", each txn will read a key ending with "initialized", ignore it for the validation.
		return false
	}
	if localconfig.LineageSupported() && strings.HasSuffix(key, "_prov") {
		return false
	}

	return true
}

func OccParse(resppayload *peer.ChaincodeAction) (readSnapshot uint64, readKeys []string, writeKeys []string) {
	txRWSet := &rwsetutil.TxRwSet{}
	if err := txRWSet.FromProtoBytes(resppayload.Results); err != nil {
		panic("Fail to retrieve rwset from txn payload")
	}

	readSnapshot = 0 // if there are no read keys, readSnapshot is useless. Just set it to the next block height
	if 1 < len(txRWSet.NsRwSets) {
		ns := txRWSet.NsRwSets[0]
		// For some reason, if we use the old nodejs API to deploy the chaincode,
		//   the chaincode action is from the 0-th NsRwSets.
		// If we use the latest bash cmd to deploy the chaincode,
		//   the chaincode action is from the 1-th NsRwSets.
		// ns := txRWSet.NsRwSets[1]

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
	}
	return
}
