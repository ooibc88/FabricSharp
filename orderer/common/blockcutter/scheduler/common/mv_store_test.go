/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func truth(string) bool {
	return true
}

func Iterate(store *Mvstore) {
	fmt.Println("===========================================")
	it := store.db.NewIterator(nil, nil)
	if it.First() {
		fmt.Printf("Exists first\n")
	} else {
		fmt.Printf("NO First\n")
	}
	for true {
		if opType, key, blkHeight, seq, err := parseCompositeKey(it.Key()); err != nil {
			fmt.Printf("Error %s\n", err.Error())
		} else if bytes.Compare(opType, WRITE_PREFIX) == 0 {
			fmt.Printf("WRITE_%s_%d_%d: %s\n", key, blkHeight, seq, string(it.Value()))
		} else if bytes.Compare(opType, READ_PREFIX) == 0 {
			fmt.Printf("READ_%s_%d_%d: %s\n", key, blkHeight, seq, string(it.Value()))
		} else {
			fmt.Printf("Unrecognized op type %v", opType)
		}
		if !it.Next() {
			break
		}
	}
	it.Release()
	fmt.Println("===========================================")

}

func TestMvStore(t *testing.T) {
	// Remove the file for a clean start
	path := "/tmp/leveldb"
	os.RemoveAll(path)

	store := NewMvStore(path)
	keyA := "A"
	keyB := "B"
	keyC := "C"
	keyD := "D"
	keyE := "E"

	blk := uint64(0)
	reads := map[string][]string{
		keyD: []string{"tx0"},
	}
	updates := map[string][]string{}
	assert.NoError(t, store.Commit(blk, updates, reads))

	blk = uint64(1)
	updates = map[string][]string{
		keyD: []string{"tx1", "tx2"},
	}
	reads = map[string][]string{}
	assert.NoError(t, store.Commit(blk, updates, reads))

	blk = uint64(2)
	updates = map[string][]string{}
	reads = map[string][]string{
		keyD: []string{"tx3", "tx4"},
	}
	assert.NoError(t, store.Commit(blk, updates, reads))

	blk = uint64(4)
	updates = map[string][]string{
		keyB: []string{"tx5", "tx6"},
	}
	reads = map[string][]string{}
	assert.NoError(t, store.Commit(blk, updates, reads))

	blk = uint64(5)
	updates = map[string][]string{
		keyB: []string{"tx7", "tx8"},
	}
	reads = map[string][]string{}
	assert.NoError(t, store.Commit(blk, updates, reads))

	/* Expected Updated Records:
	B_4_0: tx5,
	B_4_1: tx6,
	B_5_0: tx7,
	B_5_1: tx8,
	D_1_0: tx1,
	D_1_1: tx2

	Read Records:
	B_4_0: nil,
	B_5_0: nil,
	D_0_1: tx0
	D_1_0: nil
	D_2_1: tx3,
	D_2_2: tx4
	*/
	// Iterate all keys
	// Iterate(store)

	_, found := store.LastUpdatedTxnNoLaterThanBlk(2, keyA, func(string) bool { return true })
	assert.False(t, found)

	txn, found := store.LastUpdatedTxnNoLaterThanBlk(4, keyB, func(string) bool { return true })
	assert.True(t, found)
	assert.Equal(t, "tx6", txn)

	txn, found = store.LastUpdatedTxnNoLaterThanBlk(5, keyB, func(string) bool { return true })
	assert.True(t, found)
	assert.Equal(t, "tx8", txn)

	txn, found = store.LastUpdatedTxnNoLaterThanBlk(10, keyB, func(string) bool { return true })
	assert.True(t, found)
	assert.Equal(t, "tx8", txn)

	_, found = store.LastUpdatedTxnNoLaterThanBlk(1, keyC, func(string) bool { return true })
	assert.False(t, found)

	_, found = store.LastUpdatedTxnNoLaterThanBlk(0, keyD, func(string) bool { return true })
	assert.False(t, found)

	txn, found = store.LastUpdatedTxnNoLaterThanBlk(1, keyD, func(string) bool { return true })
	assert.True(t, found)
	assert.Equal(t, "tx2", txn)

	_, found = store.LastUpdatedTxnNoLaterThanBlk(0, keyE, func(string) bool { return true })
	assert.False(t, found)

	empty := NewTxnSet()
	assert.Equal(t, empty, store.UpdatedTxnsNoEarlierThanBlk(3, keyA, func(string) bool { return true }))

	assert.Equal(t, NewTxnSetWith("tx5", "tx6", "tx7", "tx8"), store.UpdatedTxnsNoEarlierThanBlk(4, keyB, func(string) bool { return true }))

	assert.Equal(t, NewTxnSetWith("tx7", "tx8"), store.UpdatedTxnsNoEarlierThanBlk(5, keyB, func(string) bool { return true }))

	assert.Equal(t, empty, store.UpdatedTxnsNoEarlierThanBlk(6, keyB, func(string) bool { return true }))

	assert.Equal(t, empty, store.UpdatedTxnsNoEarlierThanBlk(2, keyC, func(string) bool { return true }))

	assert.Equal(t, NewTxnSetWith("tx1", "tx2"), store.UpdatedTxnsNoEarlierThanBlk(0, keyD, func(string) bool { return true }))

	assert.Equal(t, NewTxnSetWith("tx1", "tx2"), store.UpdatedTxnsNoEarlierThanBlk(1, keyD, func(string) bool { return true }))

	assert.Equal(t, empty, store.UpdatedTxnsNoEarlierThanBlk(2, keyD, func(string) bool { return true }))

	assert.Equal(t, empty, store.ReadTxnsEarlierThanBlk(2, keyA, func(string) bool { return true }))
	assert.Equal(t, empty, store.ReadTxnsEarlierThanBlk(6, keyB, func(string) bool { return true }))
	assert.Equal(t, empty, store.ReadTxnsEarlierThanBlk(3, keyC, func(string) bool { return true }))

	assert.Equal(t, NewTxnSetWith("tx3", "tx4"), store.ReadTxnsEarlierThanBlk(3, keyD, func(string) bool { return true }))
	assert.Equal(t, NewTxnSetWith("tx3", "tx4"), store.ReadTxnsNoEarlierThanBlk(0, keyD))
	assert.Equal(t, NewTxnSetWith("tx3", "tx4"), store.ReadTxnsNoEarlierThanBlk(1, keyD))
	assert.Equal(t, NewTxnSetWith("tx3", "tx4"), store.ReadTxnsNoEarlierThanBlk(2, keyD))
	assert.Equal(t, empty, store.ReadTxnsNoEarlierThanBlk(3, keyD))

	assert.Equal(t, empty, store.ReadTxnsEarlierThanBlk(1, keyE, func(string) bool { return true }))
	assert.Equal(t, empty, store.ReadTxnsEarlierThanBlk(3, keyE, func(string) bool { return true }))

	updateBatch := map[string]string{"A": "a", "B": "b", "C": "c"}
	assert.NoError(t, store.BatchUpdate(updateBatch))
	assert.Equal(t, "a", store.Get("A"))
	assert.Equal(t, "", store.Get("D"))
}
