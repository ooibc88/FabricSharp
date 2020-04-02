package latestscheduler

import (
	"testing"

	"github.com/hyperledger/fabric/orderer/common/blockcutter/scheduler/common"
	"github.com/stretchr/testify/assert"
)

func TestScheduler(t *testing.T) {
	scheduler := initTxnScheduler()
	empty := []string{}
	var fakeSnapshot, fakeHeight uint64 = 0, 0
	txn1, txn2, txn3, txn4, txn5 := "T1", "T2", "T3", "T4", "T5"
	assert.True(t, scheduler.ProcessTxn([]string{"A"}, empty, fakeSnapshot, fakeHeight, txn1))
	assert.True(t, scheduler.ProcessTxn([]string{"B"}, []string{"A", "D"}, fakeSnapshot, fakeHeight, txn2))
	assert.True(t, scheduler.ProcessTxn([]string{"C"}, []string{"B"}, fakeSnapshot, fakeHeight, txn3))
	assert.True(t, scheduler.ProcessTxn([]string{"D", "E"}, []string{"C", "E"}, fakeSnapshot, fakeHeight, txn4))
	assert.True(t, scheduler.ProcessTxn([]string{"E"}, []string{"E"}, fakeSnapshot, fakeHeight, txn5))

	pred, succ := scheduler.computeDep()
	expectedSucc := map[string]common.TxnSet{
		txn1: common.NewTxnSetWith(txn2),
		txn2: common.NewTxnSetWith(txn3),
		txn3: common.NewTxnSetWith(txn4),
		txn4: common.NewTxnSetWith(txn2, txn5),
		txn5: common.NewTxnSetWith(txn4),
	}

	expectedPred := map[string]common.TxnSet{
		txn2: common.NewTxnSetWith(txn1, txn4),
		txn3: common.NewTxnSetWith(txn2),
		txn4: common.NewTxnSetWith(txn3, txn5),
		txn5: common.NewTxnSetWith(txn4),
	}

	assert.Equal(t, expectedPred, pred)
	assert.Equal(t, expectedSucc, succ)

	schedule := scheduler.ProcessBlk(fakeHeight)
	assert.Equal(t, []string{txn1, txn2, txn3, txn5}, schedule)
}
